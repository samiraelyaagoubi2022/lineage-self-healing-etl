# Changement simule 2 : impact predit et impact observe

- Date : 2026-11-28 00:20 UTC
- Prediction : `dbt ls --select state:modified+ --state /opt/airflow/dbt/state_ref`
- Observation : `dbt build` sur la copie essai.duckdb, compare a un build sans changement (`dbt/essais/base.json`)
- Build sans changement : 37 pass, 14 success, 9 warn
- Build avec le changement : 2 error, 29 pass, 11 skipped, 12 success, 6 warn

## Verdict

AUCUN OMIS : les 13 noeuds reellement impactes sont tous dans le perimetre predit, qui contient 4 noeud(s) de trop (perimetre maximal, pas exact).

- Predit : 17 noeuds (3 modele(s), 0 seed(s), 14 test(s))
- Observe : 13 noeuds (2 modele(s), 0 seed(s), 11 test(s))
- Observes et predits : 13 ; omis : 0 ; predits mais inchanges : 4
- Part du perimetre predit reellement touchee : 76 %

## Reellement impactes et predits (13)

- model `comparaison_versions_par_region` : success -> skipped
- model `dim_etablissement_education` : success -> skipped
- test `accepted_values_dim_etablissement_education_niveau__primaire_public__college_public__qualifiant_public__prive` : pass -> skipped
- test `accepted_values_dim_etablissement_education_version_source__2023__2026` : pass -> skipped
- test `assert_ecart_versions_plausible` : pass -> skipped
- test `assert_pas_de_doublon_etablissement` : warn (12 ligne(s) en defaut) -> skipped
- test `not_null_comparaison_versions_par_region_region` : pass -> skipped
- test `not_null_dim_etablissement_education_etablissement` : warn (2 ligne(s) en defaut) -> skipped
- test `not_null_dim_etablissement_education_region` : pass -> skipped
- test `not_null_dim_etablissement_education_region_code` : pass -> skipped
- test `not_null_stg_education_prive_2026_region` : pass -> error
- test `relationships_dim_etablissement_education_region__region__ref_dim_region_` : pass -> skipped
- test `relationships_stg_education_prive_2026_region__region__ref_dim_region_` : warn (1654 ligne(s) en defaut) -> error

## Impactes mais NON predits (omis) (0)

- aucun

## Predits mais inchanges (4)

- model `stg_education_prive_2026` : success -> success
- test `assert_empreinte_staging_egale_provenance` : pass -> pass
- test `assert_staging_conserve_les_lignes` : pass -> pass
- test `not_null_stg_education_prive_2026_etablissement` : pass -> pass

## Lecture

- Impacte = statut different (ou nombre de lignes en defaut different pour un test) entre le build sans changement
  et le build avec le changement. Un noeud ignore (skipped) apres une erreur est impacte.
- Un noeud predit mais inchange est un faux positif : le perimetre de `dbt ls` est un maximum
  (tout ce qui est en aval), pas l'ensemble des noeuds qui changent vraiment.
- Un changement silencieux (donnees modifiees sans changement de statut) n'est pas vu par cette mesure.
