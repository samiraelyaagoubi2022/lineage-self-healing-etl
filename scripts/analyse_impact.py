"""Analyse d'impact : que touche un changement ou une panne, vu par dbt et vu par Marquez ?

Lecture seule (ne modifie ni DuckDB ni Marquez). Ecrit un rapport Markdown dans
/opt/airflow/data/rapports/ (donc data/rapports/ sur le PC).

  1. dbt  : `dbt ls` avec la selection donnee (state:modified+, source:raw.<table>+, ...) -> modeles,
            seeds et tests impactes d'apres le manifest.
  2. Marquez : parcours du graphe de lineage en aval du dataset de depart.
  3. Comparaison : ce que voient les deux, et ce que l'un voit sans l'autre.

Lancement (PowerShell), 1er argument = dataset de depart dans Marquez, le reste = arguments de `dbt ls` :
  Get-Content scripts\\analyse_impact.py | docker compose exec -T airflow python - lineage.raw.education_qualifiant_public_2023 --select source:raw.education_qualifiant_public_2023+
  Get-Content scripts\\analyse_impact.py | docker compose exec -T airflow python - lineage.staging.stg_education_prive_2026 --select state:modified+ --state /opt/airflow/dbt/state_ref
"""
import os
import subprocess
import sys
from datetime import datetime, timezone

import requests

B = "http://marquez-api:5000/api/v1"
NS_DS = "duckdb:///opt/airflow/warehouse/lineage.duckdb"
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"
SORTIE = "/opt/airflow/data/rapports"

if len(sys.argv) < 3:
    raise SystemExit("usage : python - <dataset marquez> <arguments de dbt ls, ex. --select state:modified+ --state ...>")
depart = sys.argv[1]
args_dbt = sys.argv[2:]
nom_depart = depart.split(".")[-1]


def dbt_ls(type_ressource):
    """Noms des noeuds d'un type (model, seed, test) retenus par la selection dbt."""
    cmd = [DBT_BIN, "ls", "--quiet", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR,
           "--resource-type", type_ressource, "--output", "name"] + args_dbt
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=DBT_DIR)
    if r.returncode != 0:
        raise SystemExit(f"dbt ls a echoue ({type_ressource}) :\n{r.stdout}\n{r.stderr}")
    return sorted({ligne.strip() for ligne in r.stdout.splitlines()
                   if ligne.strip() and " " not in ligne.strip()})


def marquez_aval(nom_dataset):
    """Datasets et jobs en aval d'un dataset (parcours des arcs sortants du graphe de lineage)."""
    prefixe = f"dataset:{NS_DS}:"
    nid = prefixe + nom_dataset
    r = requests.get(f"{B}/lineage", params={"nodeId": nid, "depth": 20}, timeout=30)
    if r.status_code != 200:
        return None, None, f"depart introuvable dans Marquez (HTTP {r.status_code})"
    noeuds = {n["id"]: n for n in r.json().get("graph", [])}
    if nid not in noeuds:
        return None, None, "depart absent du graphe renvoye par Marquez"
    vus, file = {nid}, [nid]
    while file:
        courant = file.pop(0)
        for arc in noeuds[courant].get("outEdges", []):
            suivant = arc["destination"]
            if suivant not in vus and suivant in noeuds:
                vus.add(suivant)
                file.append(suivant)
    datasets = sorted(i[len(prefixe):] for i in vus if i.startswith(prefixe) and i != nid)
    jobs = sorted(i.split(":", 2)[2] for i in vus if i.startswith("job:"))
    return datasets, jobs, None


def puces(valeurs):
    return "\n".join(f"- `{v}`" for v in valeurs) if valeurs else "- aucun"


# 1) dbt
dbt_modeles = dbt_ls("model")
dbt_seeds = dbt_ls("seed")
dbt_tests = dbt_ls("test")
if not (dbt_modeles or dbt_seeds or dbt_tests):
    print("ATTENTION : la selection dbt ne retient aucun noeud "
          "(selection erronee, ou aucun fichier modifie par rapport a --state).")

# 2) Marquez
mq_datasets, mq_jobs, erreur_mq = marquez_aval(depart)
mq_modeles = {d.split(".")[-1] for d in (mq_datasets or [])}
jobs_run = [j for j in (mq_jobs or []) if j.endswith(".build.run")]
jobs_test = [j for j in (mq_jobs or []) if j.endswith(".build.test")]
jobs_autres = [j for j in (mq_jobs or []) if j not in jobs_run and j not in jobs_test]

# 3) Comparaison (le noeud de depart lui-meme n'est pas compte comme impacte "en aval")
cote_dbt = set(dbt_modeles) - {nom_depart}
communs = sorted(cote_dbt & mq_modeles)
seulement_dbt = sorted(cote_dbt - mq_modeles)
seulement_mq = sorted(mq_modeles - cote_dbt)

maintenant = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
selection = " ".join(args_dbt)
if erreur_mq:
    bloc_marquez = f"**Marquez ne peut pas suivre ce depart : {erreur_mq}.**"
else:
    bloc_marquez = (f"Datasets en aval ({len(mq_datasets)}) :\n{puces(mq_datasets)}\n\n"
                    f"Jobs en aval : {len(jobs_run)} `.build.run`, {len(jobs_test)} `.build.test`"
                    + (f", {len(jobs_autres)} autre(s)" if jobs_autres else "") + "\n"
                    + puces(jobs_run + jobs_autres))

rapport = f"""# Analyse d'impact : `{depart}`

- Date : {maintenant}
- Depart dans Marquez : `{depart}` (namespace `{NS_DS}`)
- Selection dbt : `dbt ls {selection}`

## Vu par dbt (manifest)

Modeles impactes ({len(dbt_modeles)}) :
{puces(dbt_modeles)}

Seeds ({len(dbt_seeds)}) :
{puces(dbt_seeds)}

Tests ({len(dbt_tests)}) : {len(dbt_tests)} test(s) concerne(s)

## Vu par Marquez (graphe de lineage)

{bloc_marquez}

## Comparaison (modeles en aval du depart)

- Vus par dbt et par Marquez ({len(communs)}) :
{puces(communs)}
- Vus seulement par dbt ({len(seulement_dbt)}) :
{puces(seulement_dbt)}
- Vus seulement par Marquez ({len(seulement_mq)}) :
{puces(seulement_mq)}

## Lecture

- "Seulement dbt" : dependances que Marquez ne voit pas (par exemple le seed `dim_region`, absent de Marquez).
  Pour ce depart, l'impact reel est celui de dbt.
- "Seulement Marquez" : a examiner, un dataset en aval que le manifest ne declare pas.
- Les deux listes identiques : le graphe Marquez suffit pour ce depart.
"""

os.makedirs(SORTIE, exist_ok=True)
chemin = os.path.join(SORTIE, f"impact_{depart.replace('.', '_')}.md")
with open(chemin, "w", encoding="utf-8") as f:
    f.write(rapport)
print(rapport)
print(f"Rapport ecrit : {chemin}")