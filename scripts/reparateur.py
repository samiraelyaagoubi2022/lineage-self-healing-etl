"""Réparateur à règles v1 (retry, alias, cast) sur COPIE de la base. Usage : python - [P01 ...]"""
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

import duckdb
import yaml

BASE = "/opt/airflow/warehouse/lineage.duckdb"
DIR = "/opt/airflow/warehouse/banc"
ESSAI = f"{DIR}/essai.duckdb"
JOURNAL = f"{DIR}/reparations.jsonl"
YAML = "/tmp/banc/pannes.yml"
DBT = "/home/airflow/dbt_venv/bin/dbt"
PY = "/home/airflow/dbt_venv/bin/python"
PROJET = "/opt/airflow/dbt"
MAX_TENTATIVES = 3   # plafond : 3 builds par panne, réparations comprises
ATTENTE_S = 30
assert ESSAI != BASE  # sandbox : on ne touche jamais à la vraie base


def nom(uid):
    return uid.split(".")[2]


def journal(evt):
    evt["ts"] = datetime.now(timezone.utc).isoformat()
    with open(JOURNAL, "a", encoding="utf-8") as f:
        f.write(json.dumps(evt, ensure_ascii=False) + "\n")


def schema(db):
    con = duckdb.connect(db, read_only=True)
    rows = con.execute("select table_name, column_name, data_type from information_schema.columns "
                       "where table_schema='raw' and table_name<>'_provenance'").fetchall()
    con.close()
    d = {}
    for t, c, ty in rows:
        d.setdefault(t, {})[c] = ty
    return d


def ecarts(ref, cur):
    out = []
    for t, cols in ref.items():
        now = cur.get(t, {})
        man = [c for c in cols if c not in now]
        nou = [c for c in now if c not in cols]
        typ = [(c, cols[c], now[c]) for c in cols if c in now and cols[c] != now[c]]
        if man or nou or typ:
            out.append({"table": t, "manquantes": man, "nouvelles": nou, "types": typ, "cols": now, "ref": cols})
    return out


def planifier(ecs):
    """Renvoie (cause, regle, [sql]) ou (cause, None, []) si on escalade."""
    actions, regle, cause = [], None, None
    for e in ecs:
        t = e["table"]
        if len(e["manquantes"]) == 1 and len(e["nouvelles"]) == 1 and not e["types"]:
            old, new = e["manquantes"][0], e["nouvelles"][0]
            if e["ref"][old] != e["cols"][new]:
                return "ecart_inconnu", None, []
            actions.append(f'ALTER TABLE raw."{t}" RENAME COLUMN "{new}" TO "{old}"')
            regle, cause = "alias", "colonne_renommee"
        elif e["manquantes"] and not e["nouvelles"]:
            return "colonne_supprimee", None, []
        elif e["types"] and not e["manquantes"] and not e["nouvelles"]:
            for c, avant, apres in e["types"]:
                if avant == "VARCHAR" and apres == "BLOB":
                    actions.append(f'ALTER TABLE raw."{t}" ALTER COLUMN "{c}" SET DATA TYPE VARCHAR USING decode("{c}")')
                    regle, cause = "cast", "type_modifie"
                else:
                    return "ecart_inconnu", None, []
        else:
            return "ecart_inconnu", None, []
    return cause, regle, actions


def classer_test(noms):
    if any("empreinte" in n for n in noms):
        return "empreinte_incoherente", "ne_pas_reparer"
    if any("conserve_les_lignes" in n for n in noms):
        return "lignes_perdues", "escalader"
    if any(n.startswith("relationships") for n in noms):
        return "valeurs_derivees", "escalader"
    return "test_en_echec", "escalader"


def copier_base():
    os.makedirs(DIR, exist_ok=True)
    for f in (ESSAI, ESSAI + ".wal"):
        if os.path.exists(f):
            os.remove(f)
    shutil.copyfile(BASE, ESSAI)


def lancer_dbt(pid, n):
    tp = f"/tmp/rep_target_{pid}_{n}"
    shutil.rmtree(tp, ignore_errors=True)
    p = subprocess.run([DBT, "build", "--project-dir", PROJET, "--profiles-dir", PROJET,
                        "--target-path", tp, "--log-path", tp],
                       cwd=PROJET, env=dict(os.environ, DUCKDB_PATH=ESSAI),
                       capture_output=True, text=True, timeout=900)
    f = f"{tp}/run_results.json"
    res = json.load(open(f, encoding="utf-8")).get("results", []) if os.path.exists(f) else []
    return p, res


def executer(panne, ref):
    pid = panne["id"]
    copier_base()
    inj = panne.get("injection_corrigee", panne["injection"])
    locker = None
    if inj["type"] == "sql":
        con = duckdb.connect(ESSAI)
        con.execute(inj["sql"])
        con.close()
    else:
        code = ("import duckdb,time;c=duckdb.connect(r'%s');print('ok',flush=True);time.sleep(%d)"
                % (ESSAI, inj["duree_s"]))
        locker = subprocess.Popen([PY, "-c", code], stdout=subprocess.PIPE, text=True)
        locker.stdout.readline()

    decision = cause = regle = None
    tentatives = 0
    for n in range(1, MAX_TENTATIVES + 1):
        tentatives = n
        p, res = lancer_dbt(pid, n)
        if p.returncode == 0:
            decision = "reparer" if regle else "aucune_panne"
            break
        sortie = p.stdout + p.stderr
        echecs = [r for r in res if r["status"] in ("error", "fail")]
        if "Could not set lock" in sortie and not res:
            cause, regle = "verrou_transitoire", "retry"
            journal({"panne": pid, "tentative": n, "cause": cause, "regle": regle, "attente_s": ATTENTE_S})
            time.sleep(ATTENTE_S)
            continue
        if any(r["unique_id"].startswith("model.") for r in echecs):
            if locker:
                locker.terminate()
            cause2, regle2, sqls = planifier(ecarts(ref, schema(ESSAI)))
            cause = cause2
            if not regle2:
                decision = "escalader"
                journal({"panne": pid, "tentative": n, "cause": cause, "regle": None, "decision": decision})
                break
            regle = regle2
            con = duckdb.connect(ESSAI)
            for s in sqls:
                con.execute(s)
                journal({"panne": pid, "tentative": n, "cause": cause, "regle": regle, "sql": s})
            con.close()
            continue
        tests = [nom(r["unique_id"]) for r in echecs if r["unique_id"].startswith("test.")]
        if tests:
            cause, decision = classer_test(tests)
            journal({"panne": pid, "tentative": n, "cause": cause, "regle": None, "decision": decision, "tests": tests})
            break
        cause, decision = "inconnue", "escalader"
        break
    else:
        decision = "escalader"
        journal({"panne": pid, "cause": cause, "decision": "plafond_atteint"})
    if locker:
        locker.terminate()
    exp = (panne.get("reparation_attendue") or {}).get("regle")
    return {"id": pid, "cause_attendue": panne["cause_racine"], "cause_observee": cause,
            "decision_attendue": panne["decision_attendue"], "decision_observee": decision,
            "regle_attendue": exp, "regle_appliquee": regle, "tentatives": tentatives}


def main():
    cfg = yaml.safe_load(open(YAML, encoding="utf-8"))
    ids = sys.argv[1:]
    os.makedirs(DIR, exist_ok=True)
    open(JOURNAL, "w").close()
    ref = schema(BASE)
    sorties = []
    for p in cfg["pannes"]:
        if ids and p["id"] not in ids:
            continue
        print(f"== {p['id']}", flush=True)
        try:
            sorties.append(executer(p, ref))
        except Exception as e:
            sorties.append({"id": p["id"], "cause_attendue": p["cause_racine"], "cause_observee": f"script : {e}",
                            "decision_attendue": p["decision_attendue"], "decision_observee": "erreur",
                            "regle_attendue": None, "regle_appliquee": None, "tentatives": 0})
    L = ["| Id | Cause attendue | Cause observée | Décision attendue | Décision observée | Règle attendue | Règle appliquée | Builds | OK |",
         "|---|---|---|---|---|---|---|---|---|"]
    for s in sorties:
        ok = s["decision_attendue"] == s["decision_observee"] and s["cause_attendue"] == s["cause_observee"]
        L.append(f"| {s['id']} | {s['cause_attendue']} | {s['cause_observee']} | {s['decision_attendue']} | "
                 f"{s['decision_observee']} | {s['regle_attendue']} | {s['regle_appliquee']} | {s['tentatives']} | "
                 f"{'oui' if ok else 'NON'} |")
    open(f"{DIR}/reparations.md", "w", encoding="utf-8").write("\n".join(L) + "\n")
    json.dump(sorties, open(f"{DIR}/reparations.json", "w", encoding="utf-8"), indent=1, ensure_ascii=False)
    print("\n".join(L))


main()