import os

import boto3
import duckdb

from config import *

MIN_N = 30
MIN_EMP = 5


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
        SELECT id, s, e, en,
               CASE WHEN sal_raw BETWEEN 15000 AND 1000000 THEN sal_raw END AS sal
        FROM (
            SELECT row_number() OVER () AS id,
                   list_sort(list_distinct(list_filter(
                       list_transform(string_split(key_skills, ';'),
                                      x -> replace(lower(trim(x)), '1c', '1с')),
                       x -> x <> ''))) AS s,
                   coalesce(employer_id, 'v' || cast(row_number() OVER () AS varchar)) AS e,
                   employer_name AS en,
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
    con.sql("CREATE TEMP TABLE vs AS SELECT id, s, e, en, sal FROM v WHERE sal IS NOT NULL")
    con.sql("CREATE TEMP TABLE us AS SELECT id, unnest(s) AS skill FROM vs")
    con.sql("""CREATE TEMP TABLE sk AS
               SELECT u.skill, count(*) AS n, median(vs.sal) AS med
               FROM us u JOIN vs ON vs.id = u.id GROUP BY 1""")

    total_emp = con.sql("SELECT count(DISTINCT e) FROM vs").fetchone()[0]
    print(f"вакансий с зарплатой: {con.sql('SELECT count(*) FROM vs').fetchone()[0]}, "
          f"разных работодателей: {total_emp}")
    print("топ работодателей по числу вакансий с зарплатой:")
    for r in con.sql("""SELECT en, count(*) c, round(median(sal)) FROM vs
                        GROUP BY 1 ORDER BY 2 DESC LIMIT 8""").fetchall():
        print(f"  {r[0]} | вакансий={r[1]} | медиана={r[2]:.0f}")

    con.sql(f"""
        CREATE TEMP TABLE pairs AS
        SELECT a.skill AS s1, b.skill AS s2, count(*) AS n, median(vs.sal) AS med,
               count(DISTINCT vs.e) AS n_emp
        FROM us a
        JOIN us b ON a.id = b.id AND a.skill < b.skill
        JOIN vs ON vs.id = a.id
        GROUP BY 1, 2 HAVING count(*) >= {MIN_N}
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
        CREATE TEMP TABLE pairs_res AS
        SELECT p.s1, p.s2, p.s1 || ' + ' || p.s2 AS combo, p.n, p.n_emp, p.med,
               CASE WHEN m1.n >= m2.n THEN p.s1 ELSE p.s2 END AS base,
               greatest(m1.n, m2.n) AS base_n,
               CASE WHEN m1.n >= m2.n THEN m1.med ELSE m2.med END AS base_med
        FROM pairs p JOIN sk m1 ON m1.skill = p.s1 JOIN sk m2 ON m2.skill = p.s2
    """)
    con.sql("""
        CREATE TEMP TABLE triples_res AS
        SELECT t.s1, t.s2, t.s3, t.s1 || ' + ' || t.s2 || ' + ' || t.s3 AS combo, t.n, t.n_emp, t.med,
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

    def show(table, cond, order, title):
        rows = con.sql(f"""SELECT combo, n, n_emp, med, base, base_n, base_med,
                                  med - base_med, (med - base_med) / base_med * 100
                           FROM {table} WHERE {cond}
                           ORDER BY (med - base_med) {order} LIMIT 10""").fetchall()
        print(f"\n{title}")
        for r in rows:
            print(f"  {r[0]} | n={r[1]} | работодателей={r[2]} | медиана={r[3]:.0f} | "
                  f"база: {r[4]} (n={r[5]}, медиана={r[6]:.0f}) | прибавка={r[7]:+.0f} ({r[8]:+.1f}%)")

    for table in ["pairs_res", "triples_res"]:
        show(table, "1=1", "ASC", f"{table}: БЕЗ фильтра по работодателям, понижающие")
        key = "s1, s2" if table == "pairs_res" else "s1, s2, s3"
        row = con.sql(f"SELECT {key} FROM {table} ORDER BY med - base_med ASC LIMIT 1").fetchone()
        cond = " AND ".join("list_contains(s, '" + x.replace("'", "''") + "')" for x in row)
        print(f"\nработодатели в самой «дешёвой» комбинации {table}: {' + '.join(row)}")
        for r in con.sql(f"""SELECT en, count(*) c, round(median(sal)) FROM vs WHERE {cond}
                             GROUP BY 1 ORDER BY 2 DESC LIMIT 5""").fetchall():
            print(f"  {r[0]} | вакансий={r[1]} | медиана={r[2]:.0f}")

    for table in ["pairs_res", "triples_res"]:
        flt = f"n_emp >= {MIN_EMP}"
        show(table, flt, "DESC", f"{table}: с фильтром (работодателей >= {MIN_EMP}), повышающие")
        show(table, flt, "ASC", f"{table}: с фильтром (работодателей >= {MIN_EMP}), понижающие")

    os.makedirs("results/salary", exist_ok=True)
    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, region_name="us-east-1")
    for table, name in [("pairs_res", "pairs_filtered.csv"), ("triples_res", "triples_filtered.csv")]:
        con.sql(f"""COPY (SELECT combo, n, n_emp, round(med) AS median_combo, base, base_n,
                                 round(base_med) AS median_base,
                                 round(med - base_med) AS uplift,
                                 round((med - base_med) / base_med * 100, 1) AS uplift_pct
                          FROM {table} WHERE n_emp >= {MIN_EMP} ORDER BY uplift DESC)
                    TO 'results/salary/{name}' (HEADER, DELIMITER ',')""")
        s3.upload_file(f"results/salary/{name}", BUCKET, f"results/salary/{name}")
    print("\nрезультаты загружены в s3://hh-lake/results/salary/")


if __name__ == "__main__":
    main()
