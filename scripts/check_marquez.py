import requests
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"
for ns in requests.get(f"{B}/namespaces").json()["namespaces"]:
    name = ns["name"]
    q = quote(name, safe="")
    jobs = requests.get(f"{B}/namespaces/{q}/jobs", params={"limit": 200}).json()["jobs"]
    datasets = requests.get(f"{B}/namespaces/{q}/datasets", params={"limit": 200}).json()["datasets"]
    print(f"\n== namespace {name} : {len(jobs)} jobs, {len(datasets)} datasets")
    for j in jobs:
        print("  job    ", j["name"])
    for d in datasets:
        print("  dataset", d["name"])