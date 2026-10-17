import argparse
import csv
import io
import json
import os
import re
import resource
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from itertools import combinations

import boto3

from config import *

CHUNK = 8 * 1024 * 1024
NAME_RE = re.compile(NAME_1C_REGEX)
csv.field_size_limit(sys.maxsize)


def s3_client():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY, region_name="us-east-1")


def norm_skill(s):
    return s.strip(" ").lower().replace("1c", "1с")


def last_boundary(buf):
    parts = buf.split(b'"')
    offset, cut = 0, 0
    for i, p in enumerate(parts):
        if i % 2 == 0:
            j = p.rfind(b"\n")
            if j != -1:
                cut = offset + j + 1
        offset += len(p) + 1
    return cut


def chunks(s3):
    size = s3.head_object(Bucket=BUCKET, Key=CSV_KEY)["ContentLength"]
    pos, carry, idx, cols = 0, b"", 0, None
    while pos < size:
        end = min(pos + CHUNK, size) - 1
        body = s3.get_object(Bucket=BUCKET, Key=CSV_KEY, Range=f"bytes={pos}-{end}")["Body"].read()
        pos = end + 1
        buf = carry + body
        cut = len(buf) if pos >= size else last_boundary(buf)
        if cut == 0:
            carry = buf
            continue
        data, carry = buf[:cut], buf[cut:]
        text = data.decode("utf-8-sig")
        if cols is None:
            header_end = text.index("\n") + 1
            header = next(csv.reader(io.StringIO(text[:header_end], newline="")))
            cols = {name: i for i, name in enumerate(header)}
            text = text[header_end:]
        yield idx, text, cols
        idx += 1


def mapper(task):
    idx, text, cols, attempt, fail_chunk = task
    if idx == fail_chunk and attempt == 1:
        raise RuntimeError("имитация отказа маппера")
    c1, c2, c3 = Counter(), Counter(), Counter()
    total = variant = with_skills = bad = 0
    need = max(cols["role"], cols["name"], cols["key_skills"])
    for row in csv.reader(io.StringIO(text, newline="")):
        total += 1
        if len(row) <= need:
            bad += 1
            continue
        name = row[cols["name"]].lower()
        if row[cols["role"]] != ROLE or not NAME_RE.search(name):
            continue
        if "битрикс" in name or "bitrix" in name:
            continue
        variant += 1
        raw = row[cols["key_skills"]]
        if not raw:
            continue
        skills = sorted({norm_skill(s) for s in raw.split(";") if s.strip(" ")})
        if not skills:
            continue
        with_skills += 1
        c1.update((s,) for s in skills)
        c2.update(combinations(skills, 2))
        c3.update(combinations(skills, 3))
    return c1, c2, c3, total, variant, with_skills, bad


def write_result(counter, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["skills", "count"])
        for key, cnt in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0])):
            w.writerow([" || ".join(key), cnt])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--fail-chunk", type=int, default=-1)
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    s3 = s3_client()
    C = [Counter(), Counter(), Counter()]
    stat = dict(total=0, variant=0, with_skills=0, bad=0, chunks=0, retries=0)

    with ProcessPoolExecutor(args.workers) as ex:
        pending, gen, exhausted = {}, chunks(s3), False

        def submit(task):
            pending[ex.submit(mapper, task)] = task

        while True:
            while not exhausted and len(pending) < 2 * args.workers:
                try:
                    idx, text, cols = next(gen)
                except StopIteration:
                    exhausted = True
                    break
                submit((idx, text, cols, 1, args.fail_chunk))
            if not pending:
                break
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for fut in done:
                task = pending.pop(fut)
                try:
                    res = fut.result()
                except Exception as e:
                    print(f"[отказ маппера] чанк {task[0]}, попытка {task[3]}: {e}")
                    if task[3] >= 3:
                        raise
                    stat["retries"] += 1
                    submit(task[:3] + (task[3] + 1,) + task[4:])
                    continue
                for k in range(3):
                    C[k].update(res[k])
                stat["total"] += res[3]
                stat["variant"] += res[4]
                stat["with_skills"] += res[5]
                stat["bad"] += res[6]
                stat["chunks"] += 1

    elapsed = time.time() - t0
    peak_main = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    peak_child = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024

    os.makedirs("results/mapreduce", exist_ok=True)
    for k in range(3):
        write_result(C[k], f"results/mapreduce/skills_{k+1}.csv")
    summary = dict(stat, distinct_1=len(C[0]), distinct_2=len(C[1]), distinct_3=len(C[2]),
                   seconds=round(elapsed, 1), peak_mb_main=round(peak_main, 1),
                   peak_mb_worker=round(peak_child, 1), workers=args.workers)
    with open("results/mapreduce/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    if not args.no_upload:
        for name in ["skills_1.csv", "skills_2.csv", "skills_3.csv", "summary.json"]:
            s3.upload_file(f"results/mapreduce/{name}", BUCKET, f"results/mapreduce/{name}")
        print("результаты загружены в s3://hh-lake/results/mapreduce/")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    for k, title in enumerate(["навыки", "пары", "тройки"]):
        print(f"\nТоп-5 ({title}):")
        for key, cnt in C[k].most_common(5):
            print(f"  {cnt:6d}  {' || '.join(key)}")


if __name__ == "__main__":
    main()
