"""Banc de pannes J9 : injecte chaque panne de banc/pannes.yml sur une COPIE de la base,
lance dbt build, compare avec la vérité terrain. Usage : python - [P01 P02 ...]"""
import json
import os
import shutil
import subprocess
import sys
import time

import duckdb
import yaml

BASE = "/opt/airflow/warehouse/lineage.duckdb"
DIR = "/opt/airflow/warehouse/banc"
ESSAI = f"{DIR}/essai.duckdb"
YAML = "/tmp/banc/pannes.yml"
DBT = "/home/airflow/dbt_venv/bin/dbt"
PY = "/home/airflow/dbt_venv/bin/python"
PROJET = "/opt/airflow/dbt"


def nom(uid):
    # model.lineage_etl.<nom> ou test.lineage_etl.<nom>.<hash>
    return uid.split(".")[2]


def copier_base():
    os.makedirs(DIR, exist_ok=True)
    for f in (ESSAI, ESSAI + ".wal"):
        if os.path.exists(f):
            os.remove(f)
    shutil.copyfile(BASE, ESSAI)


def lancer_dbt(pid):
    tp = f"/tmp/banc_target_{pid}"
    shutil.rmtree(tp, ignore_errors=True)
    env = dict(os.environ, DUCKDB_PATH=ESSAI)
    p = subprocess.run(
        [DBT, "build", "--project-dir", PROJET, "--profiles-dir", PROJET,
         "--target-path", tp, "--log-path", tp],
        cwd=PROJET, env=env, capture_output=True, text=True, timeout=900,
    )
    return p, tp


def lire_resultats(tp):
    f = f"{tp}/run_results.json"
    if not os.path.exists(f):
        return []
    return json.load(open(f, encoding="utf-8")).get("results", [])


def executer(panne):
    pid = panne["id"]
    copier_base()
    inj = panne["injection"]
    locker = None
    if inj["type"] == "sql":
        con = duckdb.connect(ESSAI)
        con.execute(inj["sql"])
        con.close()
    elif inj["type"] == "verrou_temporaire":
        code = ("import duckdb,time;c=duckdb.connect(r'%s');print('ok',flush=True);time.sleep(%d)"
                % (ESSAI, inj["duree_s"]))
        locker = subprocess.Popen([PY, "-c", code], stdout=subprocess.PIPE, text=True)
        locker.stdout.readline()
    debut = time.time()
    p, tp = lancer_dbt(pid)
    duree = round(time.time() - debut, 1)
    if locker:
        locker.terminate()
        locker.wait()

    res = lire_resultats(tp)
    echecs = [r for r in res if r["status"] in ("error", "fail")]
    sautes = [nom(r["unique_id"]) for r in res
              if r["status"] == "skipped" and r["unique_id"].startswith("model.")]
    noeuds_echec = [nom(r["unique_id"]) for r in echecs]
    if not res and p.returncode != 0:
        noeuds_echec = ["dbt_build"]
    blob = p.stdout + p.stderr + " ".join(str(r.get("message")) for r in echecs)

    attendu = panne["noeud_en_echec"]
    noeud_ok = any(n == attendu or n.startswith(attendu) for n in noeuds_echec)
    err = panne["erreur_attendue"]
    if err.startswith(("Binder Error", "IO Error")):
        erreur_ok = err.split("...")[0].strip() in blob
    else:
        erreur_ok = None  # message descriptif de test : non vérifié automatiquement
    modeles_echec = [nom(r["unique_id"]) for r in echecs if r["unique_id"].startswith("model.")]
    manquants = [n for n in panne["noeuds_impactes"] if n not in sautes + modeles_echec]
    return {
        "id": pid, "cause": panne["cause_racine"], "decision_attendue": panne["decision_attendue"],
        "noeud_attendu": attendu, "noeuds_en_echec_observes": noeuds_echec[:5],
        "noeud_ok": noeud_ok, "erreur_ok": erreur_ok,
        "impactes_manquants": manquants, "modeles_sautes": sautes,
        "returncode": p.returncode, "duree_s": duree,
        "messages": [str(r.get("message"))[:300] for r in echecs[:3]],
    }


def ok(v):
    return "n/a" if v is None else ("oui" if v else "NON")


def main():
    cfg = yaml.safe_load(open(YAML, encoding="utf-8"))
    ids = sys.argv[1:]
    pannes = [p for p in cfg["pannes"] if not ids or p["id"] in ids]
    sorties = []
    for p in pannes:
        print(f"== {p['id']} : {p['description']}", flush=True)
        try:
            sorties.append(executer(p))
        except Exception as e:  # une panne qui plante ne doit pas arrêter le banc
            sorties.append({"id": p["id"], "cause": p["cause_racine"],
                            "decision_attendue": p["decision_attendue"],
                            "noeud_attendu": p["noeud_en_echec"], "noeuds_en_echec_observes": [],
                            "noeud_ok": False, "erreur_ok": False, "impactes_manquants": [],
                            "modeles_sautes": [], "returncode": -1, "duree_s": 0,
                            "messages": [f"script : {e}"]})
    lignes = ["| Id | Cause | Nœud attendu | Nœuds en échec observés | Nœud ok | Erreur ok | Impact manquant | Décision attendue |",
              "|---|---|---|---|---|---|---|---|"]
    for s in sorties:
        lignes.append("| {id} | {cause} | {noeud_attendu} | {obs} | {n} | {e} | {m} | {d} |".format(
            id=s["id"], cause=s["cause"], noeud_attendu=s["noeud_attendu"],
            obs=", ".join(s["noeuds_en_echec_observes"]) or "aucun",
            n=ok(s["noeud_ok"]), e=ok(s["erreur_ok"]),
            m=", ".join(s["impactes_manquants"]) or "-", d=s["decision_attendue"]))
    os.makedirs(DIR, exist_ok=True)
    with open(f"{DIR}/resultats.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lignes) + "\n")
    with open(f"{DIR}/resultats.json", "w", encoding="utf-8") as f:
        json.dump(sorties, f, indent=1, ensure_ascii=False)
    print("\n".join(lignes))
    print(f"\nÉcrit dans {DIR}/resultats.md et resultats.json")


main()