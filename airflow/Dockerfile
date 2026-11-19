# Image Airflow du projet. Version fixée pour que les deux membres du binôme aient le même environnement.
FROM apache/airflow:2.10.5

# Intégration OpenLineage (envoie le lineage des DAG vers Marquez).
# Ingestion : DuckDB et lecture XLSX dans l'environnement Airflow.
RUN pip install --no-cache-dir apache-airflow-providers-openlineage \
    duckdb==1.2.2 openpyxl

# dbt : venv isolé (évite les conflits de dépendances avec Airflow), versions fixées.
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core==1.9.4 dbt-duckdb==1.9.4 duckdb==1.2.2 openlineage-dbt

# Dossier de l'entrepôt DuckDB : créé ici pour que le volume nommé hérite des droits de l'utilisateur airflow.
RUN mkdir -p /opt/airflow/warehouse