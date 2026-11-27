import requests
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"
NS_JOB = "projet"
NS_DS = "duckdb:///opt/airflow/warehouse/lineage.duckdb"

TABLES = [
    "education_primaire_public_2023", "education_primaire_public_2026",
    "education_college_public_2023", "education_college_public_2026",
    "education_qualifiant_public_2023", "education_qualifiant_public_2026",
    "education_prive_2023", "education_prive_2026", "sante_hopitaux_2025",
]

ok = 0
for t in TABLES:
    job = f"ingestion_data_gov_ma.charger_fichier.{t}"
    dataset = f"lineage.raw.{t}"

    r = requests.get(f"{B}/namespaces/{NS_JOB}/jobs/{quote(job, safe='')}")
    if r.status_code != 200:
        print(f"MANQUE  {t} : job introuvable ({r.status_code})")
        continue
    j = r.json()
    sorties = [o["name"] for o in j.get("outputs", [])]
    etat = (j.get("latestRun") or {}).get("state", "?")

    # Le graphe doit relier la table brute au modèle dbt staging.
    nid = quote(f"dataset:{NS_DS}:{dataset}", safe="")
    g = requests.get(f"{B}/lineage", params={"nodeId": f"dataset:{NS_DS}:{dataset}", "depth": 3})
    noeuds = [n["id"] for n in g.json().get("graph", [])] if g.status_code == 200 else []
    aval = any(f"stg_{t}" in n for n in noeuds)

    bon = dataset in sorties and etat == "COMPLETED" and aval
    ok += bon
    print(f"{'OK    ' if bon else 'PROBLEME'} {t} | sortie déclarée : {dataset in sorties} | "
          f"dernier run : {etat} | lien vers stg_ : {aval}")

print(f"\n{ok}/{len(TABLES)} tables correctes")