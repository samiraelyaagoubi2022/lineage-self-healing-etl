import json, requests
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"
NS = "projet"
m = json.load(open("/opt/airflow/dbt/target/manifest.json"))


def ds(n):
    return f"lineage.{n['schema']}.{n['name']}"


manque = 0
for uid, n in m["nodes"].items():
    if n["resource_type"] != "model":
        continue
    attendu = set()
    for dep in n["depends_on"]["nodes"]:
        d = m["nodes"].get(dep) or m["sources"].get(dep)
        if d:
            attendu.add(ds(d))
    job = f"dbt-run-lineage_etl.lineage.{n['schema']}.lineage_etl.{n['name']}.build.run"
    r = requests.get(f"{B}/namespaces/{NS}/jobs/{quote(job, safe='')}")
    if r.status_code != 200:
        print("job introuvable :", job)
        continue
    vu = {i["name"] for i in r.json().get("inputs", [])}
    ecart = attendu - vu
    if ecart:
        manque += 1
        print(n["name"], "-> dépendances dbt absentes de Marquez :", sorted(ecart))
print("modèles avec des dépendances manquantes :", manque)