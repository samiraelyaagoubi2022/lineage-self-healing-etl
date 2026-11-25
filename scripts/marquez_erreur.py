import requests

B = "http://marquez-api:5000/api/v1"
RUNS = {
    "dbt_build (DAG)": "01a21450-13d0-787c-b63b-67a91924343e",
    "dbt build.run (modèle)": "01a21450-9205-7b1b-a91c-30ba2a7041be",
}
for nom, rid in RUNS.items():
    resp = requests.get(f"{B}/jobs/runs/{rid}")
    print("==", nom, resp.status_code)
    if resp.status_code != 200:
        print("   réponse brute :", resp.text[:300])
        continue
    r = resp.json()
    print("   état :", r.get("state"))
    facets = r.get("facets") or {}
    print("   facets présents :", list(facets))
    err = facets.get("errorMessage")
    if err:
        print("   message :", err.get("message"))
        print("   stackTrace :", str(err.get("stackTrace"))[:600])