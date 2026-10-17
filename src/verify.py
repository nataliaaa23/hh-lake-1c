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
for k, title in [(1, "навыки"), (2, "пары"), (3, "тройки")]:
    mr = load("mapreduce", k)
    dd = load("duckdb", k)
    only_mr = set(mr) - set(dd)
    only_dd = set(dd) - set(mr)
    diff = [key for key in set(mr) & set(dd) if mr[key] != dd[key]]
    ok = not only_mr and not only_dd and not diff
    all_ok = all_ok and ok
    print(f"{title}: MapReduce={len(mr)}, DuckDB={len(dd)}, "
          f"только в MR={len(only_mr)}, только в DuckDB={len(only_dd)}, "
          f"разные счётчики={len(diff)}, сумма MR={sum(mr.values())}, сумма DuckDB={sum(dd.values())}, "
          f"совпало={ok}")

for method in ["mapreduce", "duckdb"]:
    body = s3.get_object(Bucket=BUCKET, Key=f"results/{method}/summary.json")["Body"].read()
    print(method, json.loads(body))
print("ИТОГ: результаты полностью совпали" if all_ok else "ИТОГ: есть расхождения")
