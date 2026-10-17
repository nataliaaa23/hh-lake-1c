import json
import os
import resource
import time

import boto3
import duckdb

from config import *


def main():
    t0 = time.time()
    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    host = ENDPOINT.split("//")[1]
    con.sql(f"""SET s3_endpoint='{host}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
                SET s3_access_key_id='{ACCESS_KEY}';
                SET s3_secret_access_key='{SECRET_KEY}';
                SET s3_region='us-east-1';""")

    con.sql(f"""
        CREATE TEMP TABLE v AS
        SELECT id, s FROM (
            SELECT row_number() OVER () AS id,
                   list_sort(list_distinct(list_filter(
                       list_transform(string_split(key_skills, ';'),
                                      x -> replace(lower(trim(x)), '1c', '1с')),
                       x -> x <> ''))) AS s
            FROM read_csv('s3://{BUCKET}/{CSV_KEY}', header=true, all_varchar=true)
            WHERE {VARIANT_SQL} AND key_skills IS NOT NULL
        ) WHERE len(s) > 0
    """)
    con.sql("CREATE TEMP TABLE u AS SELECT id, unnest(s) AS skill FROM v")

    queries = {
        1: "SELECT skill AS skills, count(*) AS count FROM u GROUP BY 1 ORDER BY 2 DESC, 1",
        2: """SELECT a.skill || ' || ' || b.skill AS skills, count(*) AS count
              FROM u a JOIN u b ON a.id = b.id AND a.skill < b.skill
              GROUP BY 1 ORDER BY 2 DESC, 1""",
        3: """SELECT a.skill || ' || ' || b.skill || ' || ' || c.skill AS skills, count(*) AS count
              FROM u a
              JOIN u b ON a.id = b.id AND a.skill < b.skill
              JOIN u c ON a.id = c.id AND b.skill < c.skill
              GROUP BY 1 ORDER BY 2 DESC, 1""",
    }

    os.makedirs("results/duckdb", exist_ok=True)
    distinct = {}
    for k, q in queries.items():
        con.sql(f"COPY ({q}) TO 'results/duckdb/skills_{k}.csv' (HEADER, DELIMITER ',')")
        distinct[k] = con.sql(f"SELECT count(*) FROM read_csv('results/duckdb/skills_{k}.csv')").fetchone()[0]

    with_skills = con.sql("SELECT count(*) FROM v").fetchone()[0]
    elapsed = time.time() - t0
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    summary = dict(with_skills=with_skills, distinct_1=distinct[1], distinct_2=distinct[2],
                   distinct_3=distinct[3], seconds=round(elapsed, 1), peak_mb=round(peak, 1))
    with open("results/duckdb/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, region_name="us-east-1")
    for name in ["skills_1.csv", "skills_2.csv", "skills_3.csv", "summary.json"]:
        s3.upload_file(f"results/duckdb/{name}", BUCKET, f"results/duckdb/{name}")
    print("результаты загружены в s3://hh-lake/results/duckdb/")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
