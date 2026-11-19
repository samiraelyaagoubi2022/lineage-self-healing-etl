import requests, csv, sys

BASE = "https://data.gov.ma/data/api/3/action"   # à vérifier

def search(query, rows=50):
    r = requests.get(f"{BASE}/package_search",
                     params={"q": query, "rows": rows}, timeout=30)
    r.raise_for_status()
    return r.json()["result"]["results"]

def main():
    queries = sys.argv[1:] or ["population", "region", "province"]
    out = []
    for q in queries:
        for ds in search(q):
            for res in ds.get("resources", []):
                out.append({
                    "query": q,
                    "dataset": ds.get("name"),
                    "titre": ds.get("title"),
                    "organisme": (ds.get("organization") or {}).get("title"),
                    "modifie": ds.get("metadata_modified"),
                    "format": res.get("format"),
                    "url": res.get("url"),
                })
    with open("exploration_ckan.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out[0].keys())
        w.writeheader(); w.writerows(out)
    print(f"{len(out)} ressources écrites")

main()