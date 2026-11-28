"""Analyse d'impact : que touche un changement ou une panne, vu par dbt et vu par Marquez ?

Lecture seule (ne modifie ni DuckDB ni Marquez). Ecrit un rapport Markdown dans
/opt/airflow/data/rapports/ (donc data/rapports/ sur le PC).

  1. dbt     : `dbt ls` avec la selection donnee -> modeles, seeds et tests impactes (le PERIMETRE).
  2. Marquez : graphe de lineage en aval du dataset de depart (verification croisee du perimetre).
  3. Etat    : etat du dernier run, et message d'erreur du dernier echec, des jobs concernes :
               job(s) producteur(s) du depart (ex. le job d'ingestion) et job dbt de chaque modele impacte.
               Les jobs dbt sont retrouves par leur nom : cela marche meme si le depart est absent de Marquez.
  4. Comparaison : ce que voient dbt et Marquez, et ce que l'un voit sans l'autre.

Lancement (PowerShell), 1er argument = dataset de depart dans Marquez, le reste = arguments de `dbt ls` :
  Get-Content scripts\\analyse_impact.py | docker compose exec -T airflow python - lineage.raw.education_qualifiant_public_2023 --select source:raw.education_qualifiant_public_2023+
  Get-Content scripts\\analyse_impact.py | docker compose exec -T airflow python - lineage.staging.stg_education_prive_2026 --select state:modified+ --state /opt/airflow/dbt/state_ref
"""
import os
import subprocess
import sys
from datetime import datetime, timezone
from urllib.parse import quote

import requests

B = "http://marquez-api:5000/api/v1"
NS_DS = "duckdb:///opt/airflow/warehouse/lineage.duckdb"
NS_JOB = "projet"
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
    """Datasets et jobs en aval d'un dataset, et jobs producteurs du dataset (arcs du graphe de lineage)."""
    prefixe = f"dataset:{NS_DS}:"
    nid = prefixe + nom_dataset
    r = requests.get(f"{B}/lineage", params={"nodeId": nid, "depth": 20}, timeout=30)
    if r.status_code != 200:
        return None, None, [], f"depart introuvable dans Marquez (HTTP {r.status_code})"
    noeuds = {n["id"]: n for n in r.json().get("graph", [])}
    if nid not in noeuds:
        return None, None, [], "depart absent du graphe renvoye par Marquez"
    producteurs = sorted({a["origin"].split(":", 2)[2] for a in noeuds[nid].get("inEdges", [])
                          if a["origin"].startswith("job:")})
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
    return datasets, jobs, producteurs, None


def tous_les_jobs():
    r = requests.get(f"{B}/namespaces/{NS_JOB}/jobs", params={"limit": 500}, timeout=30)
    r.raise_for_status()
    return [j["name"] for j in r.json()["jobs"]]


def etat_job(nom):
    """Etat du dernier run d'un job et, s'il a echoue, message d'erreur du dernier run en echec."""
    url = f"{B}/namespaces/{NS_JOB}/jobs/{quote(nom, safe='')}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return {"etat": f"introuvable (HTTP {r.status_code})", "fin": "", "erreur": ""}
    dernier = r.json().get("latestRun") or {}
    etat = dernier.get("state", "aucun run")
    fin = str(dernier.get("endedAt") or dernier.get("updatedAt") or "")[:19]
    erreur = ""
    if etat == "FAILED":
        runs = requests.get(f"{url}/runs", params={"limit": 5}, timeout=30).json().get("runs", [])
        for run in runs:
            if run.get("state") == "FAILED":
                message = ((run.get("facets") or {}).get("errorMessage") or {}).get("message")
                if message:
                    erreur = str(message)[:200]
                break
    return {"etat": etat, "fin": fin, "erreur": erreur}


def puces(valeurs):
    return "\n".join(f"- `{v}`" for v in valeurs) if valeurs else "- aucun"


def cellule(texte):
    return str(texte or "").replace("|", "/").replace("\n", " ").strip()


# 1) dbt : le perimetre
dbt_modeles = dbt_ls("model")
dbt_seeds = dbt_ls("seed")
dbt_tests = dbt_ls("test")
if not (dbt_modeles or dbt_seeds or dbt_tests):
    print("ATTENTION : la selection dbt ne retient aucun noeud "
          "(selection erronee, ou aucun fichier modifie par rapport a --state).")

# 2) Marquez : graphe en aval et producteurs du depart
mq_datasets, mq_jobs, producteurs, erreur_mq = marquez_aval(depart)
mq_modeles = {d.split(".")[-1] for d in (mq_datasets or [])}
jobs_run = [j for j in (mq_jobs or []) if j.endswith(".build.run")]
jobs_test = [j for j in (mq_jobs or []) if j.endswith(".build.test") or ".test." in j]
jobs_autres = [j for j in (mq_jobs or []) if j not in jobs_run and j not in jobs_test]

# 3) Marquez : etat des jobs concernes (producteurs du depart + un job dbt par modele du perimetre dbt)
noms_jobs = tous_les_jobs()
lignes_etat = [("producteur du depart", nom, etat_job(nom)) for nom in producteurs]
for modele in dbt_modeles:
    job = next((n for n in noms_jobs if n.endswith(f".{modele}.build.run")), None)
    if job is None:
        lignes_etat.append(("modele dbt impacte", f"(aucun job Marquez pour {modele})", None))
    else:
        lignes_etat.append(("modele dbt impacte", job, etat_job(job)))
en_echec = [nom for _, nom, etat in lignes_etat if etat and etat["etat"] == "FAILED"]

if lignes_etat:
    tableau = ["| Role | Job | Dernier run | Fin | Message d'erreur |", "|---|---|---|---|---|"]
    for role, nom, etat in lignes_etat:
        if etat is None:
            tableau.append(f"| {role} | {cellule(nom)} | introuvable | | |")
        else:
            tableau.append(f"| {role} | `{cellule(nom)}` | {etat['etat']} | {etat['fin']} | {cellule(etat['erreur'])} |")
    bloc_etat = "\n".join(tableau)
else:
    bloc_etat = "- aucun job a examiner"
note_producteur = ("" if producteurs else
                   "\n- Producteur du depart inconnu : le depart est absent de Marquez, seul le perimetre dbt est connu.")
if en_echec:
    resume_echec = "Jobs en echec dans l'etat courant : " + ", ".join(f"`{n}`" for n in en_echec) + "."
else:
    resume_echec = "Aucun job en echec dans l'etat courant de Marquez."

# 4) Comparaison (le noeud de depart lui-meme n'est pas compte comme impacte "en aval")
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
                    f"Jobs en aval : {len(jobs_run)} `.build.run`, {len(jobs_test)} tests"
                    + (f", {len(jobs_autres)} autre(s)" if jobs_autres else "") + "\n"
                    + puces(jobs_run + jobs_autres))

rapport = f"""# Analyse d'impact : `{depart}`

- Date : {maintenant}
- Depart dans Marquez : `{depart}` (namespace `{NS_DS}`)
- Selection dbt : `dbt ls {selection}`

## Perimetre vu par dbt (manifest)

Modeles impactes ({len(dbt_modeles)}) :
{puces(dbt_modeles)}

Seeds ({len(dbt_seeds)}) :
{puces(dbt_seeds)}

Tests ({len(dbt_tests)}) : {len(dbt_tests)} test(s) concerne(s)

## Etat des jobs dans Marquez (dernier run connu)

{bloc_etat}

- {resume_echec}{note_producteur}
- Cet etat est celui du dernier run envoye a Marquez. Un `dbt build` lance sans `dbt-ol` (par exemple sur une
  copie de la base, pour une simulation) n'emet rien : il n'apparait pas ici.

## Graphe en aval vu par Marquez

{bloc_marquez}

## Comparaison (modeles en aval du depart)

- Vus par dbt et par Marquez ({len(communs)}) :
{puces(communs)}
- Vus seulement par dbt ({len(seulement_dbt)}) :
{puces(seulement_dbt)}
- Vus seulement par Marquez ({len(seulement_mq)}) :
{puces(seulement_mq)}

## Lecture

- Le perimetre vient de dbt (manifest) : il est complet, y compris pour les dependances que Marquez ne voit pas
  (par exemple le seed `dim_region`).
- Marquez apporte l'etat des jobs : un job producteur en `FAILED` designe la cause probable, avec son message d'erreur.
- "Seulement dbt" : dependances que Marquez ne voit pas. "Seulement Marquez" : a examiner, un dataset en aval que
  le manifest ne declare pas. Les deux listes identiques : le graphe Marquez suffit pour ce depart.
"""

os.makedirs(SORTIE, exist_ok=True)
chemin = os.path.join(SORTIE, f"impact_{depart.replace('.', '_')}.md")
with open(chemin, "w", encoding="utf-8") as f:
    f.write(rapport)
print(rapport)
print(f"Rapport ecrit : {chemin}")