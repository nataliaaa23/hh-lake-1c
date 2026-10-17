import csv
import io
import json

import boto3

from config import *

s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY, region_name="us-east-1")


def load(method, k):
    body = s3.get_object(Bucket=BUCKET, Key=f"results/{method}/skills_{k}.csv")["Body"].read().decode("utf-8")
    rows = csv.reader(io.StringIO(body, newline=""))
    next(rows)
    return {r[0]: int(r[1]) for r in rows}


all_ok = True
for other in ["mapreduce", "duckdb"]:
    for k, title in [(1, "навыки"), (2, "пары"), (3, "тройки")]:
        a = load("mapreduce_opt", k)
        b = load(other, k)
        diff = [key for key in set(a) & set(b) if a[key] != b[key]]
        ok = a.keys() == b.keys() and not diff
        all_ok = all_ok and ok
        print(f"mapreduce_opt и {other}, {title}: {len(a)} и {len(b)}, "
              f"разные счётчики={len(diff)}, сумма {sum(a.values())} и {sum(b.values())}, совпало={ok}")

body = s3.get_object(Bucket=BUCKET, Key="results/mapreduce_opt/summary.json")["Body"].read()
print("mapreduce_opt", json.loads(body))
print("ИТОГ: результаты полностью совпали" if all_ok else "ИТОГ: есть расхождения")
