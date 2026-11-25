import requests
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"
NS = "projet"
jobs = requests.get(f"{B}/namespaces/{NS}/jobs", params={"limit": 200}).json()["jobs"]
for j in jobs:
    url = f"{B}/namespaces/{NS}/jobs/{quote(j['name'], safe='')}/runs"
    for r in requests.get(url, params={"limit": 5}).json().get("runs", []):
        if r["state"] == "FAILED":
            print(j["name"], r["id"], r["state"])
            for nom, valeur in (r.get("facets") or {}).items():
                print("   facet", nom, str(valeur)[:300])