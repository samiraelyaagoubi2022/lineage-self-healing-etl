from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from lineage_utils.echecs import notifier_echec

BASH = """
set -euo pipefail
{% if params['echec_volontaire'] %}
echo "Échec volontaire demandé (aucun événement dbt ne sera émis vers Marquez)"
exit 1
{% endif %}
export PATH=/home/airflow/dbt_venv/bin:$PATH
cd /opt/airflow/dbt
dbt-ol build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt {% if params['select'] %}--select {{ params['select'] }}{% endif %}
"""

with DAG(
    dag_id="dbt_build",
    description="dbt-ol build seul, avec callback d'échec (B, J5-J6)",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_args={
        "owner": "B",
        "retries": 0,
        "execution_timeout": timedelta(minutes=15),
        "on_failure_callback": notifier_echec,
    },
    params={
        "echec_volontaire": Param(
            False, type="boolean",
            description="Provoque un échec pour tester le callback",
        ),
                "select": Param(
            None,
            type=["null", "string"],
            pattern=r"^[A-Za-z0-9_+@,. ]*$",
            description="Sélection dbt facultative, ex. stg_sante_hopitaux_2025+",
        ),
    },
    tags=["dbt"],
    doc_md="""
**dbt_build** : lance `dbt-ol build` (modèles, seeds, tests) et envoie les événements à Marquez.
Pool `duckdb` (1 slot) : ne tourne jamais en même temps que l'ingestion.
""",
) as dag:
    BashOperator(task_id="dbt_build", bash_command=BASH, pool="duckdb")