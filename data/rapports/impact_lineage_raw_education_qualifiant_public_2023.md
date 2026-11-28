# Analyse d'impact : `lineage.raw.education_qualifiant_public_2023`

- Date : 2026-11-27 09:30 UTC
- Depart dans Marquez : `lineage.raw.education_qualifiant_public_2023` (namespace `duckdb:///opt/airflow/warehouse/lineage.duckdb`)
- Selection dbt : `dbt ls --select source:raw.education_qualifiant_public_2023+`

## Vu par dbt (manifest)

Modeles impactes (4) :
- `agg_etablissements_par_region_2023`
- `comparaison_versions_par_region`
- `dim_etablissement_education`
- `stg_education_qualifiant_public_2023`

Seeds (0) :
- aucun

Tests (16) : 16 test(s) concerne(s)

## Vu par Marquez (graphe de lineage)

Datasets en aval (4) :
- `lineage.marts.agg_etablissements_par_region_2023`
- `lineage.marts.comparaison_versions_par_region`
- `lineage.marts.dim_etablissement_education`
- `lineage.staging.stg_education_qualifiant_public_2023`

Jobs en aval : 4 `.build.run`, 4 `.build.test`, 4 autre(s)
- `dbt-run-lineage_etl.lineage.marts.lineage_etl.agg_etablissements_par_region_2023.build.run`
- `dbt-run-lineage_etl.lineage.marts.lineage_etl.comparaison_versions_par_region.build.run`
- `dbt-run-lineage_etl.lineage.marts.lineage_etl.dim_etablissement_education.build.run`
- `dbt-run-lineage_etl.lineage.staging.lineage_etl.stg_education_qualifiant_public_2023.build.run`
- `dbt-run-lineage_etl.test.lineage_etl.assert_ecart_versions_plausible`
- `dbt-run-lineage_etl.test.lineage_etl.assert_empreinte_staging_egale_provenance`
- `dbt-run-lineage_etl.test.lineage_etl.assert_pas_de_doublon_etablissement`
- `dbt-run-lineage_etl.test.lineage_etl.assert_staging_conserve_les_lignes`

## Comparaison (modeles en aval du depart)

- Vus par dbt et par Marquez (4) :
- `agg_etablissements_par_region_2023`
- `comparaison_versions_par_region`
- `dim_etablissement_education`
- `stg_education_qualifiant_public_2023`
- Vus seulement par dbt (0) :
- aucun
- Vus seulement par Marquez (0) :
- aucun

## Lecture

- "Seulement dbt" : dependances que Marquez ne voit pas (par exemple le seed `dim_region`, absent de Marquez).
  Pour ce depart, l'impact reel est celui de dbt.
- "Seulement Marquez" : a examiner, un dataset en aval que le manifest ne declare pas.
- Les deux listes identiques : le graphe Marquez suffit pour ce depart.
