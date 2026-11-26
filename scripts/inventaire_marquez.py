"""Inventaire des événements OpenLineage reçus par Marquez, séparés par origine (Airflow, dbt, test manuel).

Répond à la question de J7 : que reçoit Marquez côté Airflow, sous quel namespace, avec quels champs,
et les tables sont-elles déclarées ? Lecture seule. Lancement dans le conteneur
(l'adresse marquez-api n'existe que dans le réseau Docker) :
    Get-Content scripts\\inventaire_marquez.py | docker compose exec -T airflow python -
"""
import os
from collections import Counter, defaultdict

import requests

BASE = os.environ.get("MARQUEZ_URL", "http://marquez-api:5000") + "/api/v1"


def get(chemin):
    r = requests.get(BASE + chemin, timeout=30)
    r.raise_for_status()
    return r.json()


def origine(nom):
    if nom.startswith("dbt-run"):
        return "dbt"
    if nom == "job_test":
        return "test manuel (jour 1)"
    return "airflow"


print("== Namespaces")
for ns in get("/namespaces")["namespaces"]:
    print("  ", ns["name"])

evenements, decalage, total = [], 0, None
while True:
    page = get(f"/events/lineage?limit=500&offset={decalage}&sortDirection=desc")
    lot = page.get("events", [])
    total = page.get("totalCount", total)
    if not lot or (evenements and lot[0] == evenements[0]):      # page vide, ou même page renvoyée
        break
    evenements += lot
    decalage += len(lot)
    if (total is not None and decalage >= total) or decalage >= 5000:
        break
print(f"\n== {len(evenements)} événements lus" + (f" sur {total}" if total is not None else ""))

par_origine = defaultdict(Counter)
for e in evenements:
    par_origine[origine(e["job"]["name"])][e["eventType"]] += 1
for o, compte in sorted(par_origine.items()):
    print(f"  {o:22} " + ", ".join(f"{t} {n}" for t, n in sorted(compte.items())))

print("\n== Jobs d'Airflow : événements par type, tables déclarées, dernier événement")
jobs = defaultdict(lambda: {"types": Counter(), "tables": 0, "namespaces": set(), "dernier": ""})
facettes_run, facettes_job = set(), set()
for e in evenements:
    nom = e["job"]["name"]
    if origine(nom) != "airflow":
        continue
    j = jobs[nom]
    j["types"][e["eventType"]] += 1
    j["tables"] += len(e.get("inputs") or []) + len(e.get("outputs") or [])
    j["namespaces"].add(e["job"].get("namespace", "?"))
    j["dernier"] = max(j["dernier"], e.get("eventTime", ""))
    facettes_run |= set((e.get("run") or {}).get("facets") or {})
    facettes_job |= set((e["job"].get("facets")) or {})
for nom, j in sorted(jobs.items()):
    types = ", ".join(f"{t} {n}" for t, n in sorted(j["types"].items()))
    print(f"  {nom}\n      namespace {sorted(j['namespaces'])} | {types} | tables déclarées : {j['tables']} | dernier : {j['dernier'][:19]}")
if not jobs:
    print("  (aucun)")

print("\n== Champs (facets) présents sur les événements d'Airflow")
print("   run :", sorted(facettes_run) or "aucun")
print("   job :", sorted(facettes_job) or "aucun")

tables_dbt = set()
for e in evenements:
    if origine(e["job"]["name"]) == "dbt":
        for d in (e.get("inputs") or []) + (e.get("outputs") or []):
            tables_dbt.add((d.get("namespace"), d.get("name")))
print(f"\n== Tables déclarées par dbt : {len(tables_dbt)}")
for ns in sorted({n for n, _ in tables_dbt}, key=str):
    print("   namespace", ns)
print("   exemple :", sorted(n for _, n in tables_dbt)[:3])
