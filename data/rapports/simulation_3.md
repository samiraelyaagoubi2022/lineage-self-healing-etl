# Changement simule 3 : impact predit et impact observe

- Date : 2026-11-28 00:21 UTC
- Prediction : `dbt ls --select state:modified+ --state /opt/airflow/dbt/state_ref`
- Observation : `dbt build` sur la copie essai.duckdb, compare a un build sans changement (`dbt/essais/base.json`)
- Build sans changement : 37 pass, 14 success, 9 warn
- Build avec le changement : 4 fail, 30 pass, 14 skipped, 11 success, 1 warn

## Verdict

AUCUN OMIS : les 24 noeuds reellement impactes sont tous dans le perimetre predit, qui contient 7 noeud(s) de trop (perimetre maximal, pas exact).

- Predit : 31 noeuds (4 modele(s), 1 seed(s), 26 test(s))
- Observe : 24 noeuds (3 modele(s), 0 seed(s), 21 test(s))
- Observes et predits : 24 ; omis : 0 ; predits mais inchanges : 7
- Part du perimetre predit reellement touchee : 77 %

## Reellement impactes et predits (24)

- model `agg_etablissements_par_region_2023` : success -> skipped
- model `comparaison_versions_par_region` : success -> skipped
- model `dim_etablissement_education` : success -> skipped
- test `accepted_values_dim_etablissement_education_niveau__primaire_public__college_public__qualifiant_public__prive` : pass -> skipped
- test `accepted_values_dim_etablissement_education_version_source__2023__2026` : pass -> skipped
- test `assert_ecart_versions_plausible` : pass -> skipped
- test `assert_pas_de_doublon_etablissement` : warn (12 ligne(s) en defaut) -> skipped
- test `not_null_agg_etablissements_par_region_2023_nb_etablissements` : pass -> skipped
- test `not_null_agg_etablissements_par_region_2023_region` : pass -> skipped
- test `not_null_agg_hopitaux_par_region_region_code` : warn (1 ligne(s) en defaut) -> pass
- test `not_null_comparaison_versions_par_region_region` : pass -> skipped
- test `not_null_dim_etablissement_education_etablissement` : warn (2 ligne(s) en defaut) -> skipped
- test `not_null_dim_etablissement_education_region` : pass -> skipped
- test `not_null_dim_etablissement_education_region_code` : pass -> skipped
- test `relationships_dim_etablissement_education_region__region__ref_dim_region_` : pass -> skipped
- test `relationships_stg_education_college_public_2023_region__region__ref_dim_region_` : pass -> fail (381 ligne(s) en defaut)
- test `relationships_stg_education_college_public_2026_region__region__ref_dim_region_` : warn (437 ligne(s) en defaut) -> pass
- test `relationships_stg_education_primaire_public_2023_region__region__ref_dim_region_` : pass -> fail (1159 ligne(s) en defaut)
- test `relationships_stg_education_primaire_public_2026_region__region__ref_dim_region_` : warn (1206 ligne(s) en defaut) -> pass
- test `relationships_stg_education_prive_2023_region__region__ref_dim_region_` : pass -> fail (1376 ligne(s) en defaut)
- test `relationships_stg_education_prive_2026_region__region__ref_dim_region_` : warn (1654 ligne(s) en defaut) -> pass
- test `relationships_stg_education_qualifiant_public_2023_region__region__ref_dim_region_` : pass -> fail (253 ligne(s) en defaut)
- test `relationships_stg_education_qualifiant_public_2026_region__region__ref_dim_region_` : warn (301 ligne(s) en defaut) -> pass
- test `relationships_stg_sante_hopitaux_2025_region__region__ref_dim_region_` : warn (27 ligne(s) en defaut) -> pass

## Impactes mais NON predits (omis) (0)

- aucun

## Predits mais inchanges (7)

- model `agg_hopitaux_par_region` : success -> success
- seed `dim_region` : success -> success
- test `not_null_agg_hopitaux_par_region_region` : pass -> pass
- test `not_null_dim_region_region` : pass -> pass
- test `not_null_dim_region_region_code` : pass -> pass
- test `unique_dim_region_region` : pass -> pass
- test `unique_dim_region_region_code` : pass -> pass

## Lecture

- Impacte = statut different (ou nombre de lignes en defaut different pour un test) entre le build sans changement
  et le build avec le changement. Un noeud ignore (skipped) apres une erreur est impacte.
- Un noeud predit mais inchange est un faux positif : le perimetre de `dbt ls` est un maximum
  (tout ce qui est en aval), pas l'ensemble des noeuds qui changent vraiment.
- Un changement silencieux (donnees modifiees sans changement de statut) n'est pas vu par cette mesure.
