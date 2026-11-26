"""Résumé des tests dbt en tableau Markdown, lu dans dbt/target/run_results.json (dernier build).

Utile pour le rapport : combien de tests passent, avertissent ou échouent, et sur combien de lignes.
Lancement dans le conteneur :
    Get-Content scripts\\resume_tests_dbt.py | docker compose exec -T airflow python -
"""
import json
import os

CHEMIN = os.environ.get("DBT_RESULTS", "/opt/airflow/dbt/target/run_results.json")
with open(CHEMIN, encoding="utf-8") as f:
    donnees = json.load(f)

tests = [r for r in donnees["results"] if r["unique_id"].startswith("test.")]
comptes = {}
for r in tests:
    comptes[r["status"]] = comptes.get(r["status"], 0) + 1

print(f"Build du {donnees['metadata']['generated_at'][:19]} (dbt {donnees['metadata']['dbt_version']}) : "
      f"{len(tests)} tests, " + ", ".join(f"{v} {k}" for k, v in sorted(comptes.items())))
print()
print("| Test | Statut | Lignes en défaut |")
print("|---|---|---|")
ordre = {"fail": 0, "error": 0, "warn": 1, "pass": 2, "skipped": 3}
for r in sorted(tests, key=lambda r: (ordre.get(r["status"], 9), r["unique_id"])):
    nom = r["unique_id"].split(".")[2]
    echec = r.get("failures")
    print(f"| {nom} | {r['status']} | {'' if echec is None else echec} |")
