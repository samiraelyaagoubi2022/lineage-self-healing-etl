"""
DAG de sequence : ingestion data.gov.ma, PUIS dbt-ol build.

DuckDB n'accepte qu'un seul ecrivain : l'ingestion et dbt ne doivent jamais
tourner en meme temps. Ce DAG les enchaine :

  ingestion (DAG ingestion_data_gov_ma, attendu jusqu'au bout)
     -> verifier_projet_dbt (saute dbt tant que dbt/dbt_project.yml n'existe pas)
        -> dbt_build (dbt-ol : envoie aussi les evenements a Marquez)

Tant que B n'a pas cree le projet dbt, seule l'ingestion tourne.
"""
import os

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/dbt_venv/bin"

TOUTES_LES_TABLES = [
    "education_primaire_public_2023",
    "education_primaire_public_2026",
    "education_college_public_2023",
    "education_college_public_2026",
    "education_qualifiant_public_2023",
    "education_qualifiant_public_2026",
    "education_prive_2023",
    "education_prive_2026",
    "sante_hopitaux_2025",
]


def projet_dbt_present():
    present = os.path.exists(os.path.join(DBT_DIR, "dbt_project.yml"))
    if not present:
        print("Pas de dbt_project.yml dans /opt/airflow/dbt : etape dbt sautee.")
    return present


@dag(
    dag_id="pipeline_education_sante",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["pipeline", "ingestion", "dbt"],
)
def pipeline_education_sante():

    ingestion = TriggerDagRunOperator(
        task_id="ingestion",
        trigger_dag_id="ingestion_data_gov_ma",
        conf={"tables": TOUTES_LES_TABLES},
        wait_for_completion=True,       # on attend la fin : pas de dbt pendant l'ecriture
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
    )

    verifier_projet_dbt = ShortCircuitOperator(
        task_id="verifier_projet_dbt",
        python_callable=projet_dbt_present,
    )

    # dbt-ol appelle `dbt` via le PATH : on place le venv dbt en tete, pour cette commande seulement.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"export PATH={DBT_BIN}:$PATH && "
            f"cd {DBT_DIR} && "
            f"dbt-ol build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
        ),
        retries=0,
    )

    ingestion >> verifier_projet_dbt >> dbt_build


pipeline_education_sante()
