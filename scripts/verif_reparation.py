import shutil
import duckdb
import yaml

BASE = "/opt/airflow/warehouse/lineage.duckdb"
ESSAI = "/opt/airflow/warehouse/banc/verif.duckdb"
T = "raw.education_college_public_2023"
cfg = yaml.safe_load(open("/tmp/banc/pannes.yml", encoding="utf-8"))
REPAR = {
    "P01": 'ALTER TABLE raw.education_college_public_2023 RENAME COLUMN "REGIONX" TO "REGION"',
    "P02": 'ALTER TABLE raw.education_college_public_2023 RENAME COLUMN "NOM_ETAB" TO "NOM_ETABL"',
    "P03": 'ALTER TABLE raw.education_college_public_2023 RENAME COLUMN "COMMUNE_NOM" TO "COMMUNE"',
    "P04": 'ALTER TABLE raw.education_college_public_2023 ALTER COLUMN "COMMUNE" SET DATA TYPE VARCHAR USING decode("COMMUNE")',
    "P05": 'ALTER TABLE raw.education_college_public_2023 ALTER COLUMN "PROVINCE" SET DATA TYPE VARCHAR USING decode("PROVINCE")',
}
for p in cfg["pannes"]:
    if p["id"] not in REPAR:
        continue
    inj = p.get("injection_corrigee", p["injection"])
    shutil.copyfile(BASE, ESSAI)
    c = duckdb.connect(ESSAI)
    c.execute(inj["sql"])
    c.execute(REPAR[p["id"]])
    c.execute(f"ATTACH '{BASE}' AS sain (READ_ONLY)")
    cols = [r[0] for r in c.execute(f"DESCRIBE {T}").fetchall()]
    liste = ", ".join(f'"{x}"' for x in cols)
    n1 = c.execute(f"SELECT count(*) FROM (SELECT {liste} FROM {T} EXCEPT SELECT {liste} FROM sain.{T})").fetchone()[0]
    n2 = c.execute(f"SELECT count(*) FROM (SELECT {liste} FROM sain.{T} EXCEPT SELECT {liste} FROM {T})").fetchone()[0]
    ordre = [r[0] for r in c.execute(f"DESCRIBE sain.{T}").fetchall()]
    print(p["id"], "lignes differentes (repare - sain):", n1, "(sain - repare):", n2,
          "colonnes identiques:", cols == ordre)
    c.close()