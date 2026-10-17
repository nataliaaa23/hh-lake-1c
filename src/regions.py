import csv
import os

import boto3
import duckdb

from config import *

AREAS = ["Москва", "Санкт-Петербург", "Екатеринбург"]
MIN_N = 30
TOP = 7


def save(name, header, rows, s3):
    os.makedirs("results/regions", exist_ok=True)
    path = f"results/regions/{name}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    s3.upload_file(path, BUCKET, f"results/regions/{name}.csv")


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
    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, region_name="us-east-1")

    con.sql(f"""
        CREATE TEMP TABLE b AS
        SELECT row_number() OVER () AS id, area, schedule, experience,
               coalesce(employer_id, 'v' || cast(row_number() OVER () AS varchar)) AS e,
               name, salary_from, salary_to, gross,
               list_sort(list_distinct(list_filter(
                   list_transform(string_split(key_skills, ';'),
                                  x -> replace(lower(trim(x)), '1c', '1с')),
                   x -> x <> ''))) AS s,
               CASE WHEN currency = 'RUR' THEN
                   coalesce((try_cast(salary_from AS DOUBLE) + try_cast(salary_to AS DOUBLE)) / 2,
                            try_cast(salary_from AS DOUBLE),
                            try_cast(salary_to AS DOUBLE))
                   / CASE WHEN gross = 'false' THEN 0.87 ELSE 1 END
               END AS sal
        FROM read_csv('s3://{BUCKET}/{CSV_KEY}', header=true, all_varchar=true)
        WHERE {VARIANT_SQL}
    """)
    con.sql("""
        CREATE TEMP TABLE vs AS
        SELECT id, area, e, sal FROM b
        WHERE len(s) > 0 AND sal BETWEEN 15000 AND 1000000
        QUALIFY row_number() OVER (
            PARTITION BY e, name, salary_from, salary_to, gross, experience, s ORDER BY id) = 1
    """)
    con.sql("CREATE TEMP TABLE us AS SELECT id, area, unnest(s) AS skill FROM b WHERE len(s) > 0")

    n_all = con.sql("SELECT count(*) FROM b WHERE len(s) > 0").fetchone()[0]
    print("вакансий варианта с навыками во всех регионах:", n_all)

    groups = [(a, "area = ?", [a]) for a in AREAS] + [("Все регионы", "1=1", [])]
    t1 = []
    for label, cond, params in groups:
        r = con.execute(f"""
            SELECT count(*), count(*) FILTER (WHERE len(s) > 0),
                   round(100.0 * count(*) FILTER (WHERE schedule = 'Удаленная работа') / count(*), 1),
                   round(100.0 * count(*) FILTER (WHERE experience = 'Нет опыта') / count(*), 1)
            FROM b WHERE {cond}""", params).fetchone()
        sal = con.execute(f"""
            SELECT count(*), round(median(sal)), round(quantile_cont(sal, 0.25)),
                   round(quantile_cont(sal, 0.75))
            FROM vs WHERE {cond}""", params).fetchone()
        t1.append([label, r[0], r[1], sal[0], int(sal[1]), int(sal[2]), int(sal[3]), r[2], r[3]])
    header1 = ["регион", "вакансий варианта", "с навыками", "с зарплатой (без дублей)",
               "медиана, руб", "Q1, руб", "Q3, руб", "удалёнка, %", "без опыта, %"]
    print("\nТаблица 1. Зарплаты и условия")
    print(" | ".join(header1))
    for row in t1:
        print(" | ".join(str(x) for x in row))
    save("table1_salary", header1, t1, s3)

    skills_rows, pairs_rows, triples_rows, lift_rows = [], [], [], []
    for a in AREAS:
        n_reg = con.execute("SELECT count(*) FROM b WHERE len(s) > 0 AND area = ?", [a]).fetchone()[0]

        print(f"\n===== {a}: вакансий с навыками {n_reg}")

        print("топ навыков:")
        for sk, c in con.execute(
                "SELECT skill, count(*) c FROM us WHERE area = ? GROUP BY 1 ORDER BY c DESC, 1 LIMIT ?",
                [a, TOP]).fetchall():
            share = round(100.0 * c / n_reg, 1)
            print(f"  {c} ({share}%) {sk}")
            skills_rows.append([a, sk, c, share])

        print("топ пар:")
        for sk, c in con.execute("""
                SELECT x.skill || ' + ' || y.skill, count(*) c
                FROM us x JOIN us y ON x.id = y.id AND x.skill < y.skill
                WHERE x.area = ? AND y.area = ? GROUP BY 1 ORDER BY c DESC, 1 LIMIT ?""",
                [a, a, TOP]).fetchall():
            share = round(100.0 * c / n_reg, 1)
            print(f"  {c} ({share}%) {sk}")
            pairs_rows.append([a, sk, c, share])

        print("топ троек:")
        for sk, c in con.execute("""
                SELECT x.skill || ' + ' || y.skill || ' + ' || z.skill, count(*) c
                FROM us x
                JOIN us y ON x.id = y.id AND x.skill < y.skill
                JOIN us z ON x.id = z.id AND y.skill < z.skill
                WHERE x.area = ? AND y.area = ? AND z.area = ?
                GROUP BY 1 ORDER BY c DESC, 1 LIMIT ?""",
                [a, a, a, TOP]).fetchall():
            share = round(100.0 * c / n_reg, 1)
            print(f"  {c} ({share}%) {sk}")
            triples_rows.append([a, sk, c, share])

        print(f"навыки с наибольшим перекосом (не менее {MIN_N} вакансий):")
        for sk, c, sh_r, sh_a, lift in con.execute(f"""
                WITH reg AS (SELECT skill, count(*) c FROM us WHERE area = ? GROUP BY 1),
                     allr AS (SELECT skill, count(*) c FROM us GROUP BY 1)
                SELECT reg.skill, reg.c, round(100.0 * reg.c / {n_reg}, 1),
                       round(100.0 * allr.c / {n_all}, 1),
                       round((1.0 * reg.c / {n_reg}) / (1.0 * allr.c / {n_all}), 2)
                FROM reg JOIN allr USING (skill)
                WHERE reg.c >= {MIN_N}
                ORDER BY 5 DESC, 1 LIMIT 8""", [a]).fetchall():
            print(f"  {sk} | в регионе {sh_r}% | по всем {sh_a}% | x{lift}")
            lift_rows.append([a, sk, c, sh_r, sh_a, lift])

    save("table2_skills", ["регион", "навык", "вакансий", "доля, %"], skills_rows, s3)
    save("table2_pairs", ["регион", "пара", "вакансий", "доля, %"], pairs_rows, s3)
    save("table2_triples", ["регион", "тройка", "вакансий", "доля, %"], triples_rows, s3)
    save("table2_lift", ["регион", "навык", "вакансий", "доля в регионе, %", "доля по всем, %", "перекос"],
         lift_rows, s3)
    print("\nрезультаты загружены в s3://hh-lake/results/regions/")


if __name__ == "__main__":
    main()
