# Analyse d'impact : `lineage.raw.sante_hopitaux_2025`

- Date : 2026-11-28 00:46 UTC
- Depart dans Marquez : `lineage.raw.sante_hopitaux_2025` (namespace `duckdb:///opt/airflow/warehouse/lineage.duckdb`)
- Selection dbt : `dbt ls --select source:raw.sante_hopitaux_2025+`

## Perimetre vu par dbt (manifest)

Modeles impactes (2) :
- `agg_hopitaux_par_region`
- `stg_sante_hopitaux_2025`

Seeds (0) :
- aucun

Tests (7) : 7 test(s) concerne(s)

## Etat des jobs dans Marquez (dernier run connu)

| Role | Job | Dernier run | Fin | Message d'erreur |
|---|---|---|---|---|
| producteur du depart | `ingestion_data_gov_ma.charger_fichier.sante_hopitaux_2025` | COMPLETED | 2026-11-27T00:00:24 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.agg_hopitaux_par_region.build.run` | COMPLETED | 2026-11-26T16:57:49 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.staging.lineage_etl.stg_sante_hopitaux_2025.build.run` | COMPLETED | 2026-11-26T16:57:30 |  |

- Aucun job en echec dans l'etat courant de Marquez.
- Cet etat est celui du dernier run envoye a Marquez. Un `dbt build` lance sans `dbt-ol` (par exemple sur une
  copie de la base, pour une simulation) n'emet rien : il n'apparait pas ici.

## Graphe en aval vu par Marquez

Datasets en aval (2) :
- `lineage.marts.agg_hopitaux_par_region`
- `lineage.staging.stg_sante_hopitaux_2025`

Jobs en aval : 2 `.build.run`, 4 tests
- `dbt-run-lineage_etl.lineage.marts.lineage_etl.agg_hopitaux_par_region.build.run`
- `dbt-run-lineage_etl.lineage.staging.lineage_etl.stg_sante_hopitaux_2025.build.run`

## Comparaison (modeles en aval du depart)

- Vus par dbt et par Marquez (2) :
- `agg_hopitaux_par_region`
- `stg_sante_hopitaux_2025`
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
