import duckdb

from config import *

GROUPS = {
    "тройка (медиана 20000)": [
        "1с: зарплата и управление персоналом",
        "erp-системы на базе 1с",
        "автоматизация процессов",
    ],
    "пара (медиана 60000)": [
        "1с: зарплата и управление персоналом",
        "автоматизация процессов",
    ],
}


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
        SELECT *, CASE WHEN sal_raw BETWEEN 15000 AND 1000000 THEN sal_raw END AS sal
        FROM (
            SELECT list_sort(list_distinct(list_filter(
                       list_transform(string_split(key_skills, ';'),
                                      x -> replace(lower(trim(x)), '1c', '1с')),
                       x -> x <> ''))) AS s,
                   employer_id, name, salary_from, salary_to, gross, experience,
                   employment, schedule, published_at, area,
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
    con.sql("CREATE TEMP TABLE vs AS SELECT * FROM v WHERE sal IS NOT NULL")

    print("вся выборка с зарплатой:")
    print(con.sql("""SELECT count(*) AS n,
                            round(100.0 * count(*) FILTER (WHERE sal < 30000) / count(*), 1) AS pct_lt_30k,
                            round(100.0 * count(*) FILTER (WHERE sal < 60000) / count(*), 1) AS pct_lt_60k,
                            count(*) FILTER (WHERE employer_id IS NULL) AS no_employer_id
                     FROM vs""").fetchall())

    for title, skills in GROUPS.items():
        cond = " AND ".join("list_contains(s, '" + x + "')" for x in skills)
        con.sql(f"CREATE OR REPLACE TEMP TABLE g AS SELECT * FROM vs WHERE {cond}")
        print(f"\n===== {title}: {' + '.join(skills)}")
        print(con.sql("""SELECT count(*) AS n, count(DISTINCT employer_id) AS employers,
                                count(DISTINCT (name, salary_from, salary_to, employer_id)) AS unique_vac,
                                count(*) FILTER (WHERE salary_to IS NULL) AS no_salary_to,
                                median(sal) AS med FROM g""").fetchall())
        for label, col in [("salary_from/to/gross", "salary_from || ' / ' || coalesce(salary_to, '-') || ' / ' || gross"),
                           ("название", "name"), ("опыт", "experience"),
                           ("занятость", "employment"), ("график", "schedule"),
                           ("месяц публикации", "substr(published_at, 1, 7)"),
                           ("работодатель (id)", "employer_id")]:
            print(f"\n{label}:")
            for r in con.sql(f"SELECT {col} AS k, count(*) c FROM g GROUP BY 1 ORDER BY c DESC LIMIT 6").fetchall():
                print(f"  {r[0]} | {r[1]}")


if __name__ == "__main__":
    main()
