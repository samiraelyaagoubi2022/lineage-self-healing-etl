# Analyse d'impact : `lineage.ref.dim_region`

- Date : 2026-11-28 00:45 UTC
- Depart dans Marquez : `lineage.ref.dim_region` (namespace `duckdb:///opt/airflow/warehouse/lineage.duckdb`)
- Selection dbt : `dbt ls --select state:modified+ --state /opt/airflow/dbt/state_ref`

## Perimetre vu par dbt (manifest)

Modeles impactes (4) :
- `agg_etablissements_par_region_2023`
- `agg_hopitaux_par_region`
- `comparaison_versions_par_region`
- `dim_etablissement_education`

Seeds (1) :
- `dim_region`

Tests (26) : 26 test(s) concerne(s)

## Etat des jobs dans Marquez (dernier run connu)

| Role | Job | Dernier run | Fin | Message d'erreur |
|---|---|---|---|---|
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.agg_etablissements_par_region_2023.build.run` | COMPLETED | 2026-11-26T16:57:48 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.agg_hopitaux_par_region.build.run` | COMPLETED | 2026-11-26T16:57:49 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.comparaison_versions_par_region.build.run` | COMPLETED | 2026-11-26T16:57:52 |  |
| modele dbt impacte | `dbt-run-lineage_etl.lineage.marts.lineage_etl.dim_etablissement_education.build.run` | COMPLETED | 2026-11-26T16:57:49 |  |

- Aucun job en echec dans l'etat courant de Marquez.
- Producteur du depart inconnu : le depart est absent de Marquez, seul le perimetre dbt est connu.
- Cet etat est celui du dernier run envoye a Marquez. Un `dbt build` lance sans `dbt-ol` (par exemple sur une
  copie de la base, pour une simulation) n'emet rien : il n'apparait pas ici.

## Graphe en aval vu par Marquez

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

- Le perimetre vient de dbt (manifest) : il est complet, y compris pour les dependances que Marquez ne voit pas
  (par exemple le seed `dim_region`).
- Marquez apporte l'etat des jobs : un job producteur en `FAILED` designe la cause probable, avec son message d'erreur.
- "Seulement dbt" : dependances que Marquez ne voit pas. "Seulement Marquez" : a examiner, un dataset en aval que
  le manifest ne declare pas. Les deux listes identiques : le graphe Marquez suffit pour ce depart.
