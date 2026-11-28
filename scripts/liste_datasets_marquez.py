import requests
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"

for nom_ns in ("projet", "duckdb:///opt/airflow/warehouse/lineage.duckdb"):
    datasets = requests.get(f"{B}/namespaces/{quote(nom_ns, safe='')}/datasets",
                            params={"limit": 500}).json().get("datasets", [])
    print(f"== {nom_ns} : {len(datasets)}")
    for d in sorted(datasets, key=lambda x: x["name"]):
        print("  ", d["name"], "| mis à jour :", str(d.get("updatedAt"))[:19])