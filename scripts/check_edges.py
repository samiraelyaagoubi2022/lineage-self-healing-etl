import requests
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"
NS = "projet"

jobs = requests.get(f"{B}/namespaces/{NS}/jobs", params={"limit": 200}).json()["jobs"]
for j in sorted(jobs, key=lambda x: x["name"]):
    d = requests.get(f"{B}/namespaces/{NS}/jobs/{quote(j['name'], safe='')}").json()
    entrees = [i["name"] for i in d.get("inputs", [])]
    sorties = [o["name"] for o in d.get("outputs", [])]
    court = j["name"].split("lineage_etl.")[-1] if "stg_" in j["name"] else j["name"]
    print(court)
    print("   entrées :", entrees)
    print("   sorties :", sorties)