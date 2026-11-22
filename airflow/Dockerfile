FROM apache/airflow:2.10.5

# OpenLineage pour Airflow, DuckDB et lecture XLSX (versions fixees)
RUN pip install --no-cache-dir apache-airflow-providers-openlineage==2.0.0 \
    duckdb==1.2.2 openpyxl==3.1.5

# dbt dans un venv isole (versions fixees)
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core==1.9.4 dbt-duckdb==1.9.4 dbt-adapters==1.16.3 dbt-common==1.27.1 \
    duckdb==1.2.2 openlineage-dbt==1.55.0

# Dossier de l'entrepot DuckDB
RUN mkdir -p /opt/airflow/warehouse