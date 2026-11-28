# Analyse d'impact : `lineage.staging.stg_education_prive_2026`

- Date : 2026-11-28 00:44 UTC
- Depart dans Marquez : `lineage.staging.stg_education_prive_2026` (namespace `duckdb:///opt/airflow/warehouse/lineage.duckdb`)
- Selection dbt : `dbt ls --select state:modified+ --state /opt/airflow/dbt/state_ref`

## Perimetre vu par dbt (manifest)

Modeles impactes (3) :
- `comparaison_versions_par_region`
- `dim_etablissement_education`
- `stg_education_prive_2026`

Seeds (0) :
- aucun

Tests (14) : 14 test(s) concerne(s)

## Etat des jobs dans Marquez (dernier run connu)

| Role | Job | Dernier run | Fin | Message d'erreur |
|---|---|---|---|---|
| producteur du depart | `dbt-run-lineage_etl.lineage.staging.lineage_etl.stg_education_prive_2026.build.run` | COMPLETED | 2026-11-26T16:57:29 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.comparaison_versions_par_region.build.run` | COMPLETED | 2026-11-26T16:57:52 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.dim_etablissement_education.build.run` | COMPLETED | 2026-11-26T16:57:49 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.staging.lineage_etl.stg_education_prive_2026.build.run` | COMPLETED | 2026-11-26T16:57:29 |  |

- Aucun job en echec dans l'etat courant de Marquez.
- Cet etat est celui du dernier run envoye a Marquez. Un `dbt build` lance sans `dbt-ol` (par exemple sur une
  copie de la base, pour une simulation) n'emet rien : il n'apparait pas ici.

## Graphe en aval vu par Marquez

Datasets en aval (2) :
- `lineage.marts.comparaison_versions_par_region`
- `lineage.marts.dim_etablissement_education`

Jobs en aval : 2 `.build.run`, 7 tests
- `dbt-run-lineage_etl.lineage.marts.lineage_etl.comparaison_versions_par_region.build.run`
- `dbt-run-lineage_etl.lineage.marts.lineage_etl.dim_etablissement_education.build.run`

## Comparaison (modeles en aval du depart)

- Vus par dbt et par Marquez (2) :
- `comparaison_versions_par_region`
- `dim_etablissement_education`
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
