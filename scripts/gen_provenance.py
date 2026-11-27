import glob
import os
from urllib.parse import quote

import duckdb

SORTIE = "/opt/airflow/data/provenance"
MARQUEZ_UI = "http://localhost:3000"
NS = "projet"
NS_DATASET = "duckdb:///opt/airflow/warehouse/lineage.duckdb"
GIT_SHA = os.environ.get("GIT_SHA", "inconnu")

# Une fiche sans commit n'est pas tracable : on refuse de la generer.
if GIT_SHA == "inconnu":
    raise SystemExit("GIT_SHA manquant. Lancer avec : -e GIT_SHA=$sha (sha = git rev-parse --short HEAD)")

# Le fichier DuckDB est cherché dans le dossier warehouse du conteneur.
candidats = glob.glob("/opt/airflow/warehouse/*.duckdb")
if not candidats:
    raise SystemExit("Aucun fichier .duckdb dans /opt/airflow/warehouse")
db = candidats[0]

con = duckdb.connect(db, read_only=True)
lignes = con.execute("""
    SELECT table_brute, jeu, version, url, sha256, charge_le, run_id, nb_lignes,
           organisme, licence, ckan_modifie_le
    FROM raw."_provenance" ORDER BY table_brute
""").fetchall()
con.close()


def valeur(x):
    """Une valeur vide s'affiche explicitement, pas 'None'."""
    return x if x else "non renseigné"


os.makedirs(SORTIE, exist_ok=True)
index = ["# Fiches de provenance", "", f"Commit Git : `{GIT_SHA}`", "",
         "| Table brute | Lignes | Chargée le | Fiche |", "|---|---|---|---|"]

for (table, jeu, version, url, sha, charge_le, run_id, nb, orga, licence, modifie) in lignes:
    # Jobs et dataset dans Marquez : meme nommage que dans l'inventaire.
    job_dbt = f"dbt-run-lineage_etl.lineage.staging.lineage_etl.stg_{table}.build.run"
    lien_job = f"{MARQUEZ_UI}/lineage/job/{NS}/{quote(job_dbt, safe='')}"
    job_ingestion = f"ingestion_data_gov_ma.charger_fichier.{table}"
    lien_ingestion = f"{MARQUEZ_UI}/lineage/job/{NS}/{quote(job_ingestion, safe='')}"
    lien_dataset = (f"{MARQUEZ_UI}/lineage/dataset/{quote(NS_DATASET, safe='')}/"
                    f"{quote('lineage.raw.' + table, safe='')}")
    fiche = f"""# {table}

| Champ | Valeur |
|---|---|
| Jeu de données | {jeu} |
| Version | {version} |
| Organisme | {valeur(orga)} |
| Licence | {valeur(licence)} |
| Modifié sur CKAN le | {valeur(modifie)} |
| URL source (CKAN) | {url} |
| SHA-256 du fichier | `{sha}` |
| Lignes chargées | {nb} |
| Chargé le | {charge_le} |
| Run Airflow (ingestion) | `{run_id}` |
| Commit Git du projet | `{GIT_SHA}` |
| Table DuckDB | `raw.{table}` |
| Modèle dbt aval | `stg_{table}` |
| Job d'ingestion dans Marquez | {lien_ingestion} |
| Dataset brut dans Marquez | {lien_dataset} |
| Job dbt dans Marquez | {lien_job} |
"""
    with open(os.path.join(SORTIE, f"{table}.md"), "w", encoding="utf-8") as f:
        f.write(fiche)
    index.append(f"| {table} | {nb} | {charge_le} | [{table}.md]({table}.md) |")

with open(os.path.join(SORTIE, "README.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(index) + "\n")

print(f"{len(lignes)} fiches écrites dans {SORTIE} (base : {db})")