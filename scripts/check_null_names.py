import duckdb

c = duckdb.connect('/opt/airflow/warehouse/lineage.duckdb', read_only=True)
c.sql("""
    select "NOM_ETABA", "AdresseL", "COMMUNE", "PROVINCE", "REGION"
    from raw.education_primaire_public_2023
    where "NOM_ETABL" is null or trim("NOM_ETABL") = ''
""").show(max_cols=10)