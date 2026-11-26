"""Empreinte de la base DuckDB : une ligne par table (schema, table, nombre de lignes, empreinte du contenu).

Sert à prouver qu'un pipeline rejoué donne exactement les mêmes données :
deux exécutions identiques produisent deux listes identiques.

Les colonnes qui changent à chaque chargement par construction (date de chargement, identifiant du run)
sont ignorées. Lancement dans le conteneur, sans modifier la base (lecture seule) :
    Get-Content scripts\\empreinte_base.py | docker compose exec -T airflow python -
"""
import os
import duckdb

CHEMIN = os.environ.get("DUCKDB_PATH", "/opt/airflow/warehouse/lineage.duckdb")
VOLATILES = {"_charge_le", "_run_id", "charge_le", "run_id"}   # changent à chaque run
SCHEMAS = ("raw", "staging", "marts", "ref")

c = duckdb.connect(CHEMIN, read_only=True)
tables = c.sql(
    "select table_schema, table_name from information_schema.tables "
    "where table_schema in ('raw', 'staging', 'marts', 'ref') order by 1, 2"
).fetchall()

for schema, table in tables:
    colonnes = [r[0] for r in c.sql(
        "select column_name from information_schema.columns "
        f"where table_schema = '{schema}' and table_name = '{table}' order by ordinal_position"
    ).fetchall() if r[0] not in VOLATILES]
    ligne = ", ".join(f"coalesce(\"{col}\"::varchar, '<NULL>')" for col in colonnes)
    try:
        n, empreinte = c.sql(
            f"select count(*), md5(coalesce(string_agg(h, '' order by h), '')) "
            f"from (select md5(concat_ws('|', {ligne})) as h from {schema}.\"{table}\")"
        ).fetchone()
        print(f"{schema}.{table} | lignes={n} | empreinte={empreinte}")
    except Exception as e:                      # par exemple une vue qui pointe vers une autre base
        print(f"{schema}.{table} | ERREUR : {str(e)[:80]}")
