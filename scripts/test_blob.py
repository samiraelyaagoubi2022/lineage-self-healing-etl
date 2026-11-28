import shutil
import duckdb

shutil.copyfile("/opt/airflow/warehouse/lineage.duckdb", "/tmp/test.duckdb")
c = duckdb.connect("/tmp/test.duckdb")
for col in ("COMMUNE", "PROVINCE"):
    try:
        c.execute(f'ALTER TABLE raw.education_college_public_2023 ALTER COLUMN "{col}" SET DATA TYPE BLOB USING encode("{col}")')
        print(col, "OK")
    except Exception as e:
        print(col, "ERREUR :", str(e)[:300])