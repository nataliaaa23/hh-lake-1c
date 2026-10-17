import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

from config import *

CHUNK = 8 * 1024 * 1024

s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY, region_name="us-east-1",
                  config=Config(max_pool_connections=32))
size = s3.head_object(Bucket=BUCKET, Key=CSV_KEY)["ContentLength"]
ranges = [(p, min(p + CHUNK, size) - 1) for p in range(0, size, CHUNK)]


def fetch(r):
    body = s3.get_object(Bucket=BUCKET, Key=CSV_KEY, Range=f"bytes={r[0]}-{r[1]}")["Body"].read()
    return len(body)


def run(threads):
    t0 = time.time()
    if threads == 1:
        total = sum(fetch(r) for r in ranges)
    else:
        with ThreadPoolExecutor(threads) as pool:
            total = sum(pool.map(fetch, ranges))
    sec = time.time() - t0
    print(f"потоков={threads}: {sec:.1f} с, {total / 1024 / 1024 / sec:.1f} МиБ/с, байт={total}")


for t in (1, 4, 8, 16, 1):
    run(t)
