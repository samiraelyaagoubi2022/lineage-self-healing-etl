import glob
import os
from urllib.parse import quote

import duckdb

SORTIE = "/opt/airflow/data/provenance"
MARQUEZ_UI = "http://localhost:3000"
NS = "projet"
GIT_SHA = os.environ.get("GIT_SHA", "inconnu")

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

os.makedirs(SORTIE, exist_ok=True)
index = ["# Fiches de provenance", "", f"Commit Git : `{GIT_SHA}`", "",
         "| Table brute | Lignes | Chargée le | Fiche |", "|---|---|---|---|"]

for (table, jeu, version, url, sha, charge_le, run_id, nb, orga, licence, modifie) in lignes:
    job = f"dbt-run-lineage_etl.lineage.staging.lineage_etl.stg_{table}.build.run"
    lien_job = f"{MARQUEZ_UI}/lineage/job/{NS}/{quote(job, safe='')}"
    fiche = f"""# {table}

| Champ | Valeur |
|---|---|
| Jeu de données | {jeu} |
| Version | {version} |
| Organisme | {orga} |
| Licence | {licence} |
| Modifié sur CKAN le | {modifie} |
| URL source (CKAN) | {url} |
| SHA-256 du fichier | `{sha}` |
| Lignes chargées | {nb} |
| Chargé le | {charge_le} |
| Run Airflow (ingestion) | `{run_id}` |
| Commit Git du projet | `{GIT_SHA}` |
| Table DuckDB | `raw.{table}` |
| Modèle dbt aval | `stg_{table}` |
| Job dbt dans Marquez | {lien_job} |
"""
    with open(os.path.join(SORTIE, f"{table}.md"), "w", encoding="utf-8") as f:
        f.write(fiche)
    index.append(f"| {table} | {nb} | {charge_le} | [{table}.md]({table}.md) |")

with open(os.path.join(SORTIE, "README.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(index) + "\n")

print(f"{len(lignes)} fiches écrites dans {SORTIE} (base : {db})")