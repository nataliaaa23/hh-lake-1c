import time, boto3, duckdb
from config import *

s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY, region_name="us-east-1")
head = s3.head_object(Bucket=BUCKET, Key=CSV_KEY)
print("boto3: размер объекта =", head["ContentLength"])
part = s3.get_object(Bucket=BUCKET, Key=CSV_KEY, Range="bytes=0-199")["Body"].read()
print("boto3: первые 200 байт (Range-запрос) ->", part.decode("utf-8", errors="replace")[:120].replace("\n", " "))

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")
host = ENDPOINT.split("//")[1]
con.sql(f"""SET s3_endpoint='{host}'; SET s3_use_ssl=false; SET s3_url_style='path';
            SET s3_access_key_id='{ACCESS_KEY}'; SET s3_secret_access_key='{SECRET_KEY}';
            SET s3_region='us-east-1';""")
t = f"read_csv('s3://{BUCKET}/{CSV_KEY}', header=true, all_varchar=true)"
t0 = time.time()
print("DuckDB: всего строк в CSV:", con.sql(f"SELECT count(*) FROM {t}").fetchone()[0])
print("DuckDB: вакансий варианта:", con.sql(f"SELECT count(*) FROM {t} WHERE {VARIANT_SQL}").fetchone()[0])
print("время, с:", round(time.time() - t0, 1))
