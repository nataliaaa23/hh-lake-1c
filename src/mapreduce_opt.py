import argparse
import csv
import io
import json
import os
import re
import resource
import sys
import time
from collections import Counter, deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, FIRST_COMPLETED
from itertools import combinations

import boto3
from botocore.config import Config

from config import *

CHUNK = 8 * 1024 * 1024
NAME_RE = re.compile(NAME_1C_REGEX)
csv.field_size_limit(sys.maxsize)
OUT = "results/mapreduce_opt"


def s3_client():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY, region_name="us-east-1",
                        config=Config(max_pool_connections=32))


def norm_skill(s):
    return s.strip(" ").lower().replace("1c", "1с")


def last_boundary(buf):
    j = buf.rfind(b"\n")
    while j != -1:
        if buf.count(b'"', 0, j) % 2 == 0:
            return j + 1
        j = buf.rfind(b"\n", 0, j)
    return 0


def fetch(s3, start, end):
    return s3.get_object(Bucket=BUCKET, Key=CSV_KEY, Range=f"bytes={start}-{end}")["Body"].read()


def chunks(s3, threads):
    size = s3.head_object(Bucket=BUCKET, Key=CSV_KEY)["ContentLength"]
    ranges = [(p, min(p + CHUNK, size) - 1) for p in range(0, size, CHUNK)]
    total = len(ranges)
    it = iter(ranges)
    futures = deque()
    carry, idx, cols, got = b"", 0, None, 0
    with ThreadPoolExecutor(threads) as pool:

        def push():
            r = next(it, None)
            if r is not None:
                futures.append(pool.submit(fetch, s3, r[0], r[1]))

        for _ in range(threads):
            push()
        while futures:
            body = futures.popleft().result()
            push()
            got += 1
            buf = carry + body
            cut = len(buf) if got == total else last_boundary(buf)
            if cut == 0:
                carry = buf
                continue
            data, carry = buf[:cut], buf[cut:]
            if cols is None:
                header_end = data.index(b"\n") + 1
                header_text = data[:header_end].decode("utf-8-sig")
                header = next(csv.reader(io.StringIO(header_text, newline="")))
                cols = {name: i for i, name in enumerate(header)}
                data = data[header_end:]
            yield idx, data, cols
            idx += 1


def mapper(task):
    idx, data, cols, attempt, fail_chunk = task
    if idx == fail_chunk and attempt == 1:
        raise RuntimeError("имитация отказа маппера")
    text = data.decode("utf-8")
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
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--fail-chunk", type=int, default=-1)
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    s3 = s3_client()
    C = [Counter(), Counter(), Counter()]
    stat = dict(total=0, variant=0, with_skills=0, bad=0, chunks=0, retries=0)

    with ProcessPoolExecutor(args.workers) as ex:
        pending, gen, exhausted = {}, chunks(s3, args.threads), False

        def submit(task):
            pending[ex.submit(mapper, task)] = task

        while True:
            while not exhausted and len(pending) < 2 * args.workers:
                try:
                    idx, data, cols = next(gen)
                except StopIteration:
                    exhausted = True
                    break
                submit((idx, data, cols, 1, args.fail_chunk))
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

    os.makedirs(OUT, exist_ok=True)
    for k in range(3):
        write_result(C[k], f"{OUT}/skills_{k+1}.csv")
    summary = dict(stat, distinct_1=len(C[0]), distinct_2=len(C[1]), distinct_3=len(C[2]),
                   seconds=round(elapsed, 1), peak_mb_main=round(peak_main, 1),
                   peak_mb_worker=round(peak_child, 1), workers=args.workers, threads=args.threads)
    with open(f"{OUT}/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    if not args.no_upload:
        for name in ["skills_1.csv", "skills_2.csv", "skills_3.csv", "summary.json"]:
            s3.upload_file(f"{OUT}/{name}", BUCKET, f"results/mapreduce_opt/{name}")
        print("результаты загружены в s3://hh-lake/results/mapreduce_opt/")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
