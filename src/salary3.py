import json
import os

import boto3
import duckdb

from config import *

MIN_N = 30
MIN_EMP = 5
MAX_SHARE = 0.5


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
        CREATE TEMP TABLE raw AS
        SELECT row_number() OVER () AS id,
               list_sort(list_distinct(list_filter(
                   list_transform(string_split(key_skills, ';'),
                                  x -> replace(lower(trim(x)), '1c', '1с')),
                   x -> x <> ''))) AS s,
               employer_id, name, salary_from, salary_to, gross, experience,
               CASE WHEN currency = 'RUR' THEN
                   coalesce((try_cast(salary_from AS DOUBLE) + try_cast(salary_to AS DOUBLE)) / 2,
                            try_cast(salary_from AS DOUBLE),
                            try_cast(salary_to AS DOUBLE))
                   / CASE WHEN gross = 'false' THEN 0.87 ELSE 1 END
               END AS sal_raw
        FROM read_csv('s3://{BUCKET}/{CSV_KEY}', header=true, all_varchar=true)
        WHERE {VARIANT_SQL} AND key_skills IS NOT NULL
    """)
    con.sql("""
        CREATE TEMP TABLE before_dedup AS
        SELECT id, s, coalesce(employer_id, 'v' || cast(id AS varchar)) AS e, name,
               salary_from, salary_to, gross, experience, sal_raw AS sal
        FROM raw
        WHERE len(s) > 0 AND sal_raw BETWEEN 15000 AND 1000000
    """)
    con.sql("""
        CREATE TEMP TABLE vs AS
        SELECT id, s, e, sal FROM before_dedup
        QUALIFY row_number() OVER (
            PARTITION BY e, name, salary_from, salary_to, gross, experience, s ORDER BY id) = 1
    """)

    n_before = con.sql("SELECT count(*) FROM before_dedup").fetchone()[0]
    n_after = con.sql("SELECT count(*) FROM vs").fetchone()[0]
    emp = con.sql("SELECT count(DISTINCT e) FROM vs").fetchone()[0]
    med_after = con.sql("SELECT median(sal) FROM vs").fetchone()[0]
    print(f"вакансий с зарплатой до очистки: {n_before}, после удаления дублей: {n_after}, "
          f"работодателей: {emp}, медиана после очистки: {med_after:.0f}")
    print("работодатели с наибольшим числом вакансий до очистки:")
    for r in con.sql("""SELECT e, count(*) c, count(DISTINCT (name, salary_from, salary_to, experience, s)) u
                        FROM before_dedup GROUP BY 1 ORDER BY 2 DESC LIMIT 5""").fetchall():
        print(f"  {r[0]} | вакансий={r[1]} | уникальных={r[2]}")

    con.sql("CREATE TEMP TABLE us AS SELECT id, unnest(s) AS skill FROM vs")
    con.sql("""CREATE TEMP TABLE sk AS
               SELECT u.skill, count(*) AS n, median(vs.sal) AS med
               FROM us u JOIN vs ON vs.id = u.id GROUP BY 1""")

    con.sql(f"""
        CREATE TEMP TABLE pairs AS
        SELECT a.skill AS s1, b.skill AS s2, count(*) AS n, median(vs.sal) AS med,
               count(DISTINCT vs.e) AS n_emp
        FROM us a
        JOIN us b ON a.id = b.id AND a.skill < b.skill
        JOIN vs ON vs.id = a.id
        GROUP BY 1, 2 HAVING count(*) >= {MIN_N}
    """)
    con.sql("""
        CREATE TEMP TABLE pairs_top AS
        SELECT s1, s2, max(c) * 1.0 / sum(c) AS top_share FROM (
            SELECT a.skill AS s1, b.skill AS s2, vs.e AS e, count(*) AS c
            FROM us a
            JOIN us b ON a.id = b.id AND a.skill < b.skill
            JOIN vs ON vs.id = a.id
            GROUP BY 1, 2, 3
        ) GROUP BY 1, 2
    """)
    con.sql(f"""
        CREATE TEMP TABLE triples AS
        SELECT a.skill AS s1, b.skill AS s2, c.skill AS s3, count(*) AS n, median(vs.sal) AS med,
               count(DISTINCT vs.e) AS n_emp
        FROM us a
        JOIN us b ON a.id = b.id AND a.skill < b.skill
        JOIN us c ON a.id = c.id AND b.skill < c.skill
        JOIN vs ON vs.id = a.id
        GROUP BY 1, 2, 3 HAVING count(*) >= {MIN_N}
    """)
    con.sql("""
        CREATE TEMP TABLE triples_top AS
        SELECT s1, s2, s3, max(k) * 1.0 / sum(k) AS top_share FROM (
            SELECT a.skill AS s1, b.skill AS s2, c.skill AS s3, vs.e AS e, count(*) AS k
            FROM us a
            JOIN us b ON a.id = b.id AND a.skill < b.skill
            JOIN us c ON a.id = c.id AND b.skill < c.skill
            JOIN vs ON vs.id = a.id
            GROUP BY 1, 2, 3, 4
        ) GROUP BY 1, 2, 3
    """)

    con.sql("""
        CREATE TEMP TABLE pairs_res AS
        SELECT p.s1 || ' + ' || p.s2 AS combo, p.n, p.n_emp, t.top_share, p.med,
               CASE WHEN m1.n >= m2.n THEN p.s1 ELSE p.s2 END AS base,
               greatest(m1.n, m2.n) AS base_n,
               CASE WHEN m1.n >= m2.n THEN m1.med ELSE m2.med END AS base_med
        FROM pairs p
        JOIN pairs_top t ON t.s1 = p.s1 AND t.s2 = p.s2
        JOIN sk m1 ON m1.skill = p.s1
        JOIN sk m2 ON m2.skill = p.s2
    """)
    con.sql("""
        CREATE TEMP TABLE triples_res AS
        SELECT p.s1 || ' + ' || p.s2 || ' + ' || p.s3 AS combo, p.n, p.n_emp, t.top_share, p.med,
               CASE WHEN m1.n >= m2.n AND m1.n >= m3.n THEN p.s1
                    WHEN m2.n >= m3.n THEN p.s2 ELSE p.s3 END AS base,
               greatest(m1.n, m2.n, m3.n) AS base_n,
               CASE WHEN m1.n >= m2.n AND m1.n >= m3.n THEN m1.med
                    WHEN m2.n >= m3.n THEN m2.med ELSE m3.med END AS base_med
        FROM triples p
        JOIN triples_top t ON t.s1 = p.s1 AND t.s2 = p.s2 AND t.s3 = p.s3
        JOIN sk m1 ON m1.skill = p.s1
        JOIN sk m2 ON m2.skill = p.s2
        JOIN sk m3 ON m3.skill = p.s3
    """)

    flt = f"n_emp >= {MIN_EMP} AND top_share <= {MAX_SHARE}"
    os.makedirs("results/salary", exist_ok=True)
    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, region_name="us-east-1")

    for table, name in [("pairs_res", "pairs_dedup"), ("triples_res", "triples_dedup")]:
        kept = con.sql(f"SELECT count(*) FROM {table} WHERE {flt}").fetchone()[0]
        total = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"\n{name}: комбинаций с n>={MIN_N}: {total}, после фильтров: {kept}")
        con.sql(f"""COPY (SELECT combo, n, n_emp, round(top_share, 2) AS top_share,
                                 round(med) AS median_combo, base, base_n,
                                 round(base_med) AS median_base,
                                 round(med - base_med) AS uplift,
                                 round((med - base_med) / base_med * 100, 1) AS uplift_pct
                          FROM {table} WHERE {flt} ORDER BY uplift DESC)
                    TO 'results/salary/{name}.csv' (HEADER, DELIMITER ',')""")
        s3.upload_file(f"results/salary/{name}.csv", BUCKET, f"results/salary/{name}.csv")
        for title, order in [("повышающие", "DESC"), ("понижающие", "ASC")]:
            print(f"\n{name}: топ-10 {title}")
            rows = con.sql(f"""SELECT combo, n, n_emp, top_share, med, base, base_n, base_med,
                                      med - base_med, (med - base_med) / base_med * 100
                               FROM {table} WHERE {flt}
                               ORDER BY (med - base_med) {order} LIMIT 10""").fetchall()
            for r in rows:
                print(f"  {r[0]} | n={r[1]} | работодателей={r[2]} | доля топ-работодателя={r[3]:.2f} | "
                      f"медиана={r[4]:.0f} | база: {r[5]} (n={r[6]}, медиана={r[7]:.0f}) | "
                      f"прибавка={r[8]:+.0f} ({r[9]:+.1f}%)")

    stats = dict(before=n_before, after=n_after, employers=emp, median_after=med_after,
                 min_n=MIN_N, min_emp=MIN_EMP, max_share=MAX_SHARE)
    with open("results/salary/summary_dedup.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    s3.upload_file("results/salary/summary_dedup.json", BUCKET, "results/salary/summary_dedup.json")
    print("\nрезультаты загружены в s3://hh-lake/results/salary/")


if __name__ == "__main__":
    main()
