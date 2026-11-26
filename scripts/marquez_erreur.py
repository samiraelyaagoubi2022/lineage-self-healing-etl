import requests

B = "http://marquez-api:5000/api/v1"
NS = "projet"

for job in ("dbt_build", "dbt_build.dbt_build"):
    resp = requests.get(f"{B}/namespaces/{NS}/jobs/{job}/runs", params={"limit": 5})
    print("==", job, resp.status_code)
    if resp.status_code != 200:
        continue
    for r in resp.json().get("runs", []):
        if r.get("state") != "FAILED":
            continue
        facets = r.get("facets") or {}
        print("  run", r["id"], "facets :", list(facets))
        err = facets.get("errorMessage")
        if err:
            print("    message :", err.get("message"))