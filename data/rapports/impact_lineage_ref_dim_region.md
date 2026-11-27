# Analyse d'impact : `lineage.ref.dim_region`

- Date : 2026-11-27 09:32 UTC
- Depart dans Marquez : `lineage.ref.dim_region` (namespace `duckdb:///opt/airflow/warehouse/lineage.duckdb`)
- Selection dbt : `dbt ls --select state:modified+ --state /opt/airflow/dbt/state_ref`

## Vu par dbt (manifest)

Modeles impactes (4) :
- `agg_etablissements_par_region_2023`
- `agg_hopitaux_par_region`
- `comparaison_versions_par_region`
- `dim_etablissement_education`

Seeds (1) :
- `dim_region`

Tests (26) : 26 test(s) concerne(s)

## Vu par Marquez (graphe de lineage)

**Marquez ne peut pas suivre ce depart : depart introuvable dans Marquez (HTTP 404).**

## Comparaison (modeles en aval du depart)

- Vus par dbt et par Marquez (0) :
- aucun
- Vus seulement par dbt (4) :
- `agg_etablissements_par_region_2023`
- `agg_hopitaux_par_region`
- `comparaison_versions_par_region`
- `dim_etablissement_education`
- Vus seulement par Marquez (0) :
- aucun

## Lecture

- "Seulement dbt" : dependances que Marquez ne voit pas (par exemple le seed `dim_region`, absent de Marquez).
  Pour ce depart, l'impact reel est celui de dbt.
- "Seulement Marquez" : a examiner, un dataset en aval que le manifest ne declare pas.
- Les deux listes identiques : le graphe Marquez suffit pour ce depart.
