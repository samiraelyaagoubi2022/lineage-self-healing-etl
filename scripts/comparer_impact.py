"""Compare l'impact PREDIT (dbt ls) a l'impact OBSERVE (vrai dbt build sur la copie essai.duckdb).

- Predit  : `dbt ls` avec la selection donnee (source:...+, state:modified+ ...) : modeles, seeds, tests.
- Observe : noeuds dont le statut (ou le nombre de lignes en defaut d'un test) change entre
            dbt/essais/base.json (build sans changement) et dbt/essais/change<N>.json (build avec le changement).
- Resultat : noeuds observes ET predits, predits mais inchanges, et OMIS (observes mais non predits :
            ne doit pas arriver). Ecrit data/rapports/simulation_<N>.md.

Limite : un changement silencieux (donnees modifiees sans changement de statut ni de nombre de lignes
en defaut) n'est pas vu par cette mesure.

Lancement (PowerShell) :
  Get-Content scripts\\comparer_impact.py | docker compose exec -T airflow python - 1 --select source:raw.education_qualifiant_public_2023+
  Get-Content scripts\\comparer_impact.py | docker compose exec -T airflow python - 2 --select state:modified+ --state /opt/airflow/dbt/state_ref
"""
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone

DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"
ESSAIS = f"{DBT_DIR}/essais"
SORTIE = "/opt/airflow/data/rapports"

if len(sys.argv) < 3:
    raise SystemExit("usage : python - <numero du changement> <arguments de dbt ls, ex. --select state:modified+ --state ...>")
numero = sys.argv[1]
args_dbt = sys.argv[2:]


def lire(chemin):
    """Statut (et lignes en defaut) de chaque modele, seed et test d'un run_results.json."""
    if not os.path.exists(chemin):
        raise SystemExit(f"fichier introuvable : {chemin} (le dbt build correspondant n'a pas ete fait ?)")
    with open(chemin, encoding="utf-8") as f:
        donnees = json.load(f)
    resultats = {}
    for r in donnees["results"]:
        morceaux = r["unique_id"].split(".")
        if morceaux[0] in ("model", "seed", "test"):
            resultats[(morceaux[0], morceaux[2])] = (r["status"], r.get("failures"))
    return resultats


def dbt_ls(type_ressource):
    cmd = [DBT_BIN, "ls", "--quiet", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR,
           "--resource-type", type_ressource, "--output", "name"] + args_dbt
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=DBT_DIR)
    if r.returncode != 0:
        raise SystemExit(f"dbt ls a echoue ({type_ressource}) :\n{r.stdout}\n{r.stderr}")
    return {ligne.strip() for ligne in r.stdout.splitlines() if ligne.strip() and " " not in ligne.strip()}


def fmt(etat):
    if etat is None:
        return "absent"
    statut, echecs = etat
    return f"{statut} ({echecs} ligne(s) en defaut)" if echecs else statut


predit = ({("model", n) for n in dbt_ls("model")}
          | {("seed", n) for n in dbt_ls("seed")}
          | {("test", n) for n in dbt_ls("test")})
base = lire(f"{ESSAIS}/base.json")
chg = lire(f"{ESSAIS}/change{numero}.json")
observe = {k for k in set(base) | set(chg) if base.get(k) != chg.get(k)}

communs = sorted(observe & predit)
omis = sorted(observe - predit)
inchanges = sorted(predit - observe)


def lignes(cles):
    if not cles:
        return "- aucun"
    return "\n".join(f"- {t} `{nom}` : {fmt(base.get((t, nom)))} -> {fmt(chg.get((t, nom)))}" for t, nom in cles)


def par_type(cles):
    c = Counter(t for t, _ in cles)
    return f"{c['model']} modele(s), {c['seed']} seed(s), {c['test']} test(s)"


def bilan(res):
    return ", ".join(f"{n} {s}" for s, n in sorted(Counter(v[0] for v in res.values()).items()))


if not omis and not inchanges:
    verdict = "PREDICTION EXACTE : les noeuds reellement impactes sont exactement ceux predits."
elif not omis:
    verdict = (f"AUCUN OMIS : les {len(observe)} noeuds reellement impactes sont tous dans le perimetre predit, "
               f"qui contient {len(inchanges)} noeud(s) de trop (perimetre maximal, pas exact).")
else:
    verdict = f"ECART : {len(omis)} noeud(s) reellement impacte(s) mais NON predit(s)."

precision = f"{100 * len(communs) / len(predit):.0f} %" if predit else "n/a"
maintenant = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

rapport = f"""# Changement simule {numero} : impact predit et impact observe

- Date : {maintenant}
- Prediction : `dbt ls {' '.join(args_dbt)}`
- Observation : `dbt build` sur la copie essai.duckdb, compare a un build sans changement (`dbt/essais/base.json`)
- Build sans changement : {bilan(base)}
- Build avec le changement : {bilan(chg)}

## Verdict

{verdict}

- Predit : {len(predit)} noeuds ({par_type(predit)})
- Observe : {len(observe)} noeuds ({par_type(observe)})
- Observes et predits : {len(communs)} ; omis : {len(omis)} ; predits mais inchanges : {len(inchanges)}
- Part du perimetre predit reellement touchee : {precision}

## Reellement impactes et predits ({len(communs)})

{lignes(communs)}

## Impactes mais NON predits (omis) ({len(omis)})

{lignes(omis)}

## Predits mais inchanges ({len(inchanges)})

{lignes(inchanges)}

## Lecture

- Impacte = statut different (ou nombre de lignes en defaut different pour un test) entre le build sans changement
  et le build avec le changement. Un noeud ignore (skipped) apres une erreur est impacte.
- Un noeud predit mais inchange est un faux positif : le perimetre de `dbt ls` est un maximum
  (tout ce qui est en aval), pas l'ensemble des noeuds qui changent vraiment.
- Un changement silencieux (donnees modifiees sans changement de statut) n'est pas vu par cette mesure.
"""

os.makedirs(SORTIE, exist_ok=True)
chemin = os.path.join(SORTIE, f"simulation_{numero}.md")
with open(chemin, "w", encoding="utf-8") as f:
    f.write(rapport)
print(rapport)
print(f"Rapport ecrit : {chemin}")