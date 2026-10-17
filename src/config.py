import os

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://host.docker.internal:9100")
ACCESS_KEY = "rustfsadmin"
SECRET_KEY = "rustfsadmin"
BUCKET = "hh-lake"
CSV_KEY = "raw/it_vacancies_2024.csv"

ROLE = "Программист, разработчик"
NAME_1C_REGEX = r"(^|[^0-9])1\s?[cс]"

VARIANT_SQL = (
    "role = 'Программист, разработчик' "
    "AND regexp_matches(lower(name), '(^|[^0-9])1\\s?[cс]') "
    "AND NOT (lower(name) LIKE '%битрикс%' OR lower(name) LIKE '%bitrix%')"
)
