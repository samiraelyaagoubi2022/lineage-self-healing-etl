import duckdb

c = duckdb.connect('/opt/airflow/warehouse/lineage.duckdb', read_only=True)
c.sql("select count(*) as lignes, sum(nb_etablissements) as total from marts.agg_etablissements_par_region_2023").show()
c.sql("select count(*) as lignes, sum(nb_hopitaux) as total from marts.agg_hopitaux_par_region").show()
c.sql("select region, region_code, nb_hopitaux from marts.agg_hopitaux_par_region order by region_code nulls last").show(max_rows=20)
c.sql("""
    select count(*) as lignes, count(distinct etablissement) as noms_distincts,
           count(distinct etablissement || '|' || commune) as couples_distincts
    from (select * from staging.stg_education_primaire_public_2023
          union all select * from staging.stg_education_college_public_2023
          union all select * from staging.stg_education_qualifiant_public_2023
          union all select * from staging.stg_education_prive_2023)
""").show()