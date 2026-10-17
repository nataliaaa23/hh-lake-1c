import json
import os

import boto3
import duckdb

from config import *

MIN_N = 30


def main():
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
        SELECT id, s, sal_raw,
               CASE WHEN sal_raw BETWEEN 15000 AND 1000000 THEN sal_raw END AS sal
        FROM (
            SELECT row_number() OVER () AS id,
                   list_sort(list_distinct(list_filter(
                       list_transform(string_split(key_skills, ';'),
                                      x -> replace(lower(trim(x)), '1c', '1с')),
                       x -> x <> ''))) AS s,
                   CASE WHEN currency = 'RUR' THEN
                       coalesce((try_cast(salary_from AS DOUBLE) + try_cast(salary_to AS DOUBLE)) / 2,
                                try_cast(salary_from AS DOUBLE),
                                try_cast(salary_to AS DOUBLE))
                       / CASE WHEN gross = 'false' THEN 0.87 ELSE 1 END
                   END AS sal_raw
            FROM read_csv('s3://{BUCKET}/{CSV_KEY}', header=true, all_varchar=true)
            WHERE {VARIANT_SQL} AND key_skills IS NOT NULL
        ) WHERE len(s) > 0
    """)

    total = con.sql("SELECT count(*) FROM v").fetchone()[0]
    with_rur = con.sql("SELECT count(sal_raw) FROM v").fetchone()[0]
    used = con.sql("SELECT count(sal) FROM v").fetchone()[0]
    overall = con.sql("SELECT median(sal) FROM v").fetchone()[0]
    print(f"вакансий с навыками: {total}, с зарплатой в RUR: {with_rur}, "
          f"после отсева выбросов: {used}, общая медиана: {overall:.0f}")

    con.sql("CREATE TEMP TABLE vs AS SELECT id, s, sal FROM v WHERE sal IS NOT NULL")
    con.sql("CREATE TEMP TABLE us AS SELECT id, unnest(s) AS skill FROM vs")
    con.sql("""CREATE TEMP TABLE sk AS
               SELECT u.skill, count(*) AS n, median(vs.sal) AS med
               FROM us u JOIN vs ON vs.id = u.id GROUP BY 1""")

    con.sql(f"""
        CREATE TEMP TABLE pairs AS
        SELECT a.skill AS s1, b.skill AS s2, count(*) AS n, median(vs.sal) AS med
        FROM us a
        JOIN us b ON a.id = b.id AND a.skill < b.skill
        JOIN vs ON vs.id = a.id
        GROUP BY 1, 2 HAVING count(*) >= {MIN_N}
    """)
    con.sql(f"""
        CREATE TEMP TABLE triples AS
        SELECT a.skill AS s1, b.skill AS s2, c.skill AS s3, count(*) AS n, median(vs.sal) AS med
        FROM us a
        JOIN us b ON a.id = b.id AND a.skill < b.skill
        JOIN us c ON a.id = c.id AND b.skill < c.skill
        JOIN vs ON vs.id = a.id
        GROUP BY 1, 2, 3 HAVING count(*) >= {MIN_N}
    """)

    con.sql("""
        CREATE TEMP TABLE pairs_res AS
        SELECT p.s1 || ' + ' || p.s2 AS combo, p.n, p.med,
               CASE WHEN m1.n >= m2.n THEN p.s1 ELSE p.s2 END AS base,
               greatest(m1.n, m2.n) AS base_n,
               CASE WHEN m1.n >= m2.n THEN m1.med ELSE m2.med END AS base_med
        FROM pairs p JOIN sk m1 ON m1.skill = p.s1 JOIN sk m2 ON m2.skill = p.s2
    """)
    con.sql("""
        CREATE TEMP TABLE triples_res AS
        SELECT t.s1 || ' + ' || t.s2 || ' + ' || t.s3 AS combo, t.n, t.med,
               CASE WHEN m1.n >= m2.n AND m1.n >= m3.n THEN t.s1
                    WHEN m2.n >= m3.n THEN t.s2 ELSE t.s3 END AS base,
               greatest(m1.n, m2.n, m3.n) AS base_n,
               CASE WHEN m1.n >= m2.n AND m1.n >= m3.n THEN m1.med
                    WHEN m2.n >= m3.n THEN m2.med ELSE m3.med END AS base_med
        FROM triples t
        JOIN sk m1 ON m1.skill = t.s1
        JOIN sk m2 ON m2.skill = t.s2
        JOIN sk m3 ON m3.skill = t.s3
    """)

    os.makedirs("results/salary", exist_ok=True)
    for name in ["pairs", "triples"]:
        con.sql(f"""COPY (SELECT combo, n, round(med) AS median_combo, base, base_n,
                                 round(base_med) AS median_base,
                                 round(med - base_med) AS uplift,
                                 round((med - base_med) / base_med * 100, 1) AS uplift_pct
                          FROM {name}_res ORDER BY uplift DESC)
                    TO 'results/salary/{name}.csv' (HEADER, DELIMITER ',')""")
        for title, order in [("повышают", "DESC"), ("понижают", "ASC")]:
            print(f"\n{name}: топ-10, которые {title} медиану")
            rows = con.sql(f"""SELECT combo, n, round(med), base, base_n, round(base_med),
                                      round(med - base_med), round((med - base_med) / base_med * 100, 1)
                               FROM {name}_res ORDER BY (med - base_med) {order} LIMIT 10""").fetchall()
            for r in rows:
                print(f"  {r[0]} | n={r[1]} | медиана={r[2]:.0f} | база: {r[3]} (n={r[4]}, медиана={r[5]:.0f}) "
                      f"| прибавка={r[6]:+.0f} ({r[7]:+.1f}%)")

    stats = dict(with_skills=total, with_rur_salary=with_rur, used=used, overall_median=overall,
                 min_n=MIN_N, pairs=con.sql("SELECT count(*) FROM pairs_res").fetchone()[0],
                 triples=con.sql("SELECT count(*) FROM triples_res").fetchone()[0])
    with open("results/salary/summary.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, region_name="us-east-1")
    for name in ["pairs.csv", "triples.csv", "summary.json"]:
        s3.upload_file(f"results/salary/{name}", BUCKET, f"results/salary/{name}")
    print("\nрезультаты загружены в s3://hh-lake/results/salary/")
    print(stats)


if __name__ == "__main__":
    main()
