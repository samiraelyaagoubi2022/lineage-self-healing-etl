import json
from datetime import datetime, timezone

FAILURES_FILE = "/opt/airflow/warehouse/failures.jsonl"
RUN_RESULTS = "/opt/airflow/dbt/target/run_results.json"


def lire_echecs_dbt(debut_tache):
    """Nœuds dbt en échec, seulement si run_results.json a été écrit pendant cette tâche."""
    try:
        with open(RUN_RESULTS, encoding="utf-8") as f:
            data = json.load(f)
        genere = datetime.fromisoformat(data["metadata"]["generated_at"])
        if debut_tache and genere < debut_tache:
            return {"run_results": "périmé (écrit avant cette tâche)", "genere_le": genere.isoformat()}
        return {
            "genere_le": genere.isoformat(),
            "echecs_dbt": [
                {"noeud": r["unique_id"], "statut": r["status"], "message": (r.get("message") or "")[:500]}
                for r in data.get("results", [])
                if r["status"] in ("error", "fail")
            ],
        }
    except Exception as e:
        return {"run_results": f"illisible : {e}"}


def notifier_echec(context):
    try:
        ti = context["task_instance"]
        exc = context.get("exception")
        enreg = {
            "horodatage": datetime.now(timezone.utc).isoformat(),
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": context["run_id"],
            "erreur_airflow": str(exc)[:1000] if exc else None,
            "log_url": ti.log_url,
        }
        if "dbt" in ti.task_id:
            enreg.update(lire_echecs_dbt(ti.start_date))
        with open(FAILURES_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(enreg, ensure_ascii=False) + "\n")
        print("CALLBACK ÉCHEC :", json.dumps(enreg, ensure_ascii=False))
    except Exception as e:  # le callback ne doit jamais masquer l'échec d'origine
        print("CALLBACK ÉCHEC : impossible d'enregistrer l'échec :", e)