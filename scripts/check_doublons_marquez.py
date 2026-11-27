import requests
from collections import defaultdict
from urllib.parse import quote

B = "http://marquez-api:5000/api/v1"
vus = defaultdict(list)

for ns in requests.get(f"{B}/namespaces").json()["namespaces"]:
    nom_ns = ns["name"]
    datasets = requests.get(f"{B}/namespaces/{quote(nom_ns, safe='')}/datasets",
                            params={"limit": 500}).json().get("datasets", [])
    print(f"== {nom_ns} : {len(datasets)} datasets")
    for d in datasets:
        vus[d["name"]].append(nom_ns)
        if "{{" in d["name"] or "#" in d["name"]:
            print("   SUSPECT :", d["name"])

print("\nNoms présents dans plusieurs namespaces :")
doublons = {n: ns for n, ns in vus.items() if len(ns) > 1}
for n, ns in sorted(doublons.items()):
    print("  ", n, "->", ns)
if not doublons:
    print("   aucun")