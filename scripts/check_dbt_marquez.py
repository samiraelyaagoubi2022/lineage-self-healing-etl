import json
import requests
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"
NS = "projet"

manifest = json.load(open("/opt/airflow/dbt/target/manifest.json"))
modeles = [n["name"] for n in manifest["nodes"].values() if n["resource_type"] == "model"]

jobs = requests.get(f"{B}/namespaces/{NS}/jobs", params={"limit": 500}).json()["jobs"]
jobs_run = [j for j in jobs if j["name"].endswith(".build.run")]
jobs_test = [j for j in jobs if j["name"].endswith(".build.test")]


def etat(j):
    return (j.get("latestRun") or {}).get("state", "?")


print(f"Modèles dans le manifest dbt : {len(modeles)}")
print(f"Jobs .build.run dans Marquez : {len(jobs_run)} | jobs .build.test : {len(jobs_test)}\n")

manquants = []
for m in sorted(modeles):
    j = next((x for x in jobs_run if f".{m}.build.run" in x["name"]), None)
    if j is None:
        manquants.append(m)
        print(f"MANQUE  {m}")
        continue
    d = requests.get(f"{B}/namespaces/{NS}/jobs/{quote(j['name'], safe='')}").json()
    print(f"{'OK     ' if etat(j) == 'COMPLETED' else 'ETAT   '} {m} | {etat(j)} | "
          f"entrées {len(d.get('inputs', []))} | sorties {len(d.get('outputs', []))}")

print(f"\nModèles absents de Marquez : {manquants or 'aucun'}")