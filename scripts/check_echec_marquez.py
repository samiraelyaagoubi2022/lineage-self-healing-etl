import requests
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"
NS = "projet"

jobs = requests.get(f"{B}/namespaces/{NS}/jobs", params={"limit": 500}).json()["jobs"]
for j in jobs:
    if j["name"] == "job_test":
        continue
    if (j.get("latestRun") or {}).get("state") != "FAILED":
        continue
    runs = requests.get(f"{B}/namespaces/{NS}/jobs/{quote(j['name'], safe='')}/runs",
                        params={"limit": 3}).json().get("runs", [])
    for r in runs:
        if r["state"] != "FAILED":
            continue
        print("\n==", j["name"], "| run", r["id"])
        facets = r.get("facets") or {}
        print("   facets présents :", sorted(facets))
        err = facets.get("errorMessage")
        if err:
            print("   message :", str(err.get("message"))[:300])
        for nom in ("airflowState", "parent"):
            if nom in facets:
                print(f"   {nom} :", str(facets[nom])[:200])