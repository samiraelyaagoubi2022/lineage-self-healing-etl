# Changement simule 1 : impact predit et impact observe

- Date : 2026-11-28 00:19 UTC
- Prediction : `dbt ls --select source:raw.education_qualifiant_public_2023+`
- Observation : `dbt build` sur la copie essai.duckdb, compare a un build sans changement (`dbt/essais/base.json`)
- Build sans changement : 37 pass, 14 success, 9 warn
- Build avec le changement : 1 error, 23 pass, 19 skipped, 10 success, 7 warn

## Verdict

PREDICTION EXACTE : les noeuds reellement impactes sont exactement ceux predits.

- Predit : 20 noeuds (4 modele(s), 0 seed(s), 16 test(s))
- Observe : 20 noeuds (4 modele(s), 0 seed(s), 16 test(s))
- Observes et predits : 20 ; omis : 0 ; predits mais inchanges : 0
- Part du perimetre predit reellement touchee : 100 %

## Reellement impactes et predits (20)

- model `agg_etablissements_par_region_2023` : success -> skipped
- model `comparaison_versions_par_region` : success -> skipped
- model `dim_etablissement_education` : success -> skipped
- model `stg_education_qualifiant_public_2023` : success -> error
- test `accepted_values_dim_etablissement_education_niveau__primaire_public__college_public__qualifiant_public__prive` : pass -> skipped
- test `accepted_values_dim_etablissement_education_version_source__2023__2026` : pass -> skipped
- test `assert_ecart_versions_plausible` : pass -> skipped
- test `assert_empreinte_staging_egale_provenance` : pass -> skipped
- test `assert_pas_de_doublon_etablissement` : warn (12 ligne(s) en defaut) -> skipped
- test `assert_staging_conserve_les_lignes` : pass -> skipped
- test `not_null_agg_etablissements_par_region_2023_nb_etablissements` : pass -> skipped
- test `not_null_agg_etablissements_par_region_2023_region` : pass -> skipped
- test `not_null_comparaison_versions_par_region_region` : pass -> skipped
- test `not_null_dim_etablissement_education_etablissement` : warn (2 ligne(s) en defaut) -> skipped
- test `not_null_dim_etablissement_education_region` : pass -> skipped
- test `not_null_dim_etablissement_education_region_code` : pass -> skipped
- test `not_null_stg_education_qualifiant_public_2023_etablissement` : pass -> skipped
- test `not_null_stg_education_qualifiant_public_2023_region` : pass -> skipped
- test `relationships_dim_etablissement_education_region__region__ref_dim_region_` : pass -> skipped
- test `relationships_stg_education_qualifiant_public_2023_region__region__ref_dim_region_` : pass -> skipped

## Impactes mais NON predits (omis) (0)

- aucun

## Predits mais inchanges (0)

- aucun

## Lecture

- Impacte = statut different (ou nombre de lignes en defaut different pour un test) entre le build sans changement
  et le build avec le changement. Un noeud ignore (skipped) apres une erreur est impacte.
- Un noeud predit mais inchange est un faux positif : le perimetre de `dbt ls` est un maximum
  (tout ce qui est en aval), pas l'ensemble des noeuds qui changent vraiment.
- Un changement silencieux (donnees modifiees sans changement de statut) n'est pas vu par cette mesure.
