# Analyse d'impact : `lineage.raw.education_qualifiant_public_2023`

- Date : 2026-11-28 00:41 UTC
- Depart dans Marquez : `lineage.raw.education_qualifiant_public_2023` (namespace `duckdb:///opt/airflow/warehouse/lineage.duckdb`)
- Selection dbt : `dbt ls --select source:raw.education_qualifiant_public_2023+`

## Perimetre vu par dbt (manifest)

Modeles impactes (4) :
- `agg_etablissements_par_region_2023`
- `comparaison_versions_par_region`
- `dim_etablissement_education`
- `stg_education_qualifiant_public_2023`

Seeds (0) :
- aucun

Tests (16) : 16 test(s) concerne(s)

## Etat des jobs dans Marquez (dernier run connu)

| Role | Job | Dernier run | Fin | Message d'erreur |
|---|---|---|---|---|
| producteur du depart | `ingestion_data_gov_ma.charger_fichier.education_qualifiant_public_2023` | COMPLETED | 2026-11-26T23:57:02 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.agg_etablissements_par_region_2023.build.run` | COMPLETED | 2026-11-26T16:57:48 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.comparaison_versions_par_region.build.run` | COMPLETED | 2026-11-26T16:57:52 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.dim_etablissement_education.build.run` | COMPLETED | 2026-11-26T16:57:49 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.staging.lineage_etl.stg_education_qualifiant_public_2023.build.run` | COMPLETED | 2026-11-26T16:57:29 |  |

- Aucun job en echec dans l'etat courant de Marquez.
- Cet etat est celui du dernier run envoye a Marquez. Un `dbt build` lance sans `dbt-ol` (par exemple sur une
  copie de la base, pour une simulation) n'emet rien : il n'apparait pas ici.

## Graphe en aval vu par Marquez

Datasets en aval (4) :
- `lineage.marts.agg_etablissements_par_region_2023`
- `lineage.marts.comparaison_versions_par_region`
- `lineage.marts.dim_etablissement_education`
- `lineage.staging.stg_education_qualifiant_public_2023`

Jobs en aval : 4 `.build.run`, 8 tests
- `dbt-run-lineage_etl.lineage.marts.lineage_etl.agg_etablissements_par_region_2023.build.run`
- `dbt-run-lineage_etl.lineage.marts.lineage_etl.comparaison_versions_par_region.build.run`
- `dbt-run-lineage_etl.lineage.marts.lineage_etl.dim_etablissement_education.build.run`
- `dbt-run-lineage_etl.lineage.staging.lineage_etl.stg_education_qualifiant_public_2023.build.run`

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

- Le perimetre vient de dbt (manifest) : il est complet, y compris pour les dependances que Marquez ne voit pas
  (par exemple le seed `dim_region`).
- Marquez apporte l'etat des jobs : un job producteur en `FAILED` designe la cause probable, avec son message d'erreur.
- "Seulement dbt" : dependances que Marquez ne voit pas. "Seulement Marquez" : a examiner, un dataset en aval que
  le manifest ne declare pas. Les deux listes identiques : le graphe Marquez suffit pour ce depart.
