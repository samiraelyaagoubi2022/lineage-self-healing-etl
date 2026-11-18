# Image Airflow du projet. Version fixée pour que les deux membres du binôme aient le même environnement.
FROM apache/airflow:2.10.5

# Intégration OpenLineage (envoie le lineage des DAG vers Marquez).
# dbt sera ajouté plus tard, dans un conteneur séparé, pour éviter les conflits de dépendances avec Airflow.
RUN pip install --no-cache-dir apache-airflow-providers-openlineage