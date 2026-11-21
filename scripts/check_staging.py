import duckdb

c = duckdb.connect('/opt/airflow/warehouse/lineage.duckdb', read_only=True)
vues = [r[0] for r in c.sql(
    "select table_name from information_schema.tables "
    "where table_schema = 'staging' and table_name like 'stg_education%'"
).fetchall()]
tout = " union all ".join(f"select * from staging.{v}" for v in vues)

print("Lignes par niveau et version (doit égaler les nb_lignes du tableau) :")
c.sql(f"""
    select niveau, version, count(*) as lignes,
           count(region) as avec_region, count(distinct region) as nb_regions
    from ({tout}) group by 1, 2 order by 1, 2
""").show(max_rows=20)

print("Casablanca, 2023 contre 2026 :")
c.sql(f"""
    select version, region, count(*) as n
    from ({tout}) where region like '%Casablanca%'
    group by 1, 2 order by 1
""").show(max_rows=20)