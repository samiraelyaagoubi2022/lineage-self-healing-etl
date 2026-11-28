"""Applique un changement simule (1, 2 ou 3) pour le banc d'essai d'impact (J9).

  1 : derive de schema dans une table brute : colonne REGION renommee LL_REG dans
      raw.education_qualifiant_public_2023, UNIQUEMENT dans la copie essai.duckdb.
  2 : modification de code : dans stg_education_prive_2026.sql, la colonne region devient region_nom.
  3 : modification du referentiel : dans le seed dim_region.csv, "Grand Casablanca-Settat" devient "Casablanca-Settat".

Ne touche JAMAIS lineage.duckdb. Les changements 2 et 3 modifient un fichier du projet dbt : a ANNULER
ensuite avec git checkout (la commande est affichee).

Lancement (PowerShell) :
  Get-Content scripts\\simuler_changement.py | docker compose exec -T airflow python - 1
"""
import sys

import duckdb

ESSAI = "/opt/airflow/warehouse/essai.duckdb"
DBT = "/opt/airflow/dbt"

numero = sys.argv[1] if len(sys.argv) > 1 else None


def remplacer(chemin, ancien, nouveau):
    """Remplace une occurrence unique, en conservant l'encodage et les fins de ligne du fichier."""
    with open(chemin, encoding="utf-8", newline="") as f:
        texte = f.read()
    if texte.count(ancien) != 1:
        raise SystemExit(f"{chemin} : '{ancien}' trouve {texte.count(ancien)} fois (attendu : 1). Rien n'a ete modifie.")
    with open(chemin, "w", encoding="utf-8", newline="") as f:
        f.write(texte.replace(ancien, nouveau))
    print(f"modifie : {chemin} : '{ancien}' -> '{nouveau}'")


if numero == "1":
    con = duckdb.connect(ESSAI)
    try:
        # Meme schema de remplacement que l'ingestion (CREATE OR REPLACE TABLE ... AS SELECT).
        con.execute('CREATE OR REPLACE TABLE raw.tmp_derive AS '
                    'SELECT * RENAME ("REGION" AS "LL_REG") FROM raw.education_qualifiant_public_2023')
        con.execute("CREATE OR REPLACE TABLE raw.education_qualifiant_public_2023 AS SELECT * FROM raw.tmp_derive")
        con.execute("DROP TABLE raw.tmp_derive")
    finally:
        con.close()
    print("copie essai.duckdb : REGION renommee LL_REG dans raw.education_qualifiant_public_2023")
elif numero == "2":
    remplacer(f"{DBT}/models/staging/stg_education_prive_2026.sql", "as region,", "as region_nom,")
    print("A ANNULER : git checkout -- dbt/models/staging/stg_education_prive_2026.sql")
elif numero == "3":
    remplacer(f"{DBT}/seeds/dim_region.csv", "Grand Casablanca-Settat", "Casablanca-Settat")
    print("A ANNULER : git checkout -- dbt/seeds/dim_region.csv")
else:
    raise SystemExit("usage : python - <1|2|3>")