| Id | Cause | Nœud attendu | Nœuds en échec observés | Nœud ok | Erreur ok | Impact manquant | Décision attendue |
|---|---|---|---|---|---|---|---|
| P01 | colonne_renommee | stg_education_college_public_2023 | stg_education_college_public_2023 | oui | NON | - | reparer |
| P02 | colonne_renommee | stg_education_college_public_2023 | stg_education_college_public_2023 | oui | oui | - | reparer |
| P03 | colonne_renommee | stg_education_college_public_2023 | stg_education_college_public_2023 | oui | NON | - | reparer |
| P04 | type_modifie | stg_education_college_public_2023 | aucun | NON | NON | - | reparer |
| P05 | type_modifie | stg_education_college_public_2023 | aucun | NON | NON | - | reparer |
| P06 | verrou_transitoire | dbt_build | dbt_build | oui | oui | - | reparer |
| P07 | colonne_supprimee | stg_education_college_public_2023 | stg_education_college_public_2023 | oui | NON | - | escalader |
| P08 | lignes_perdues | assert_staging_conserve_les_lignes | assert_staging_conserve_les_lignes | oui | n/a | dim_etablissement_education, comparaison_versions_par_region | escalader |
| P09 | empreinte_incoherente | assert_empreinte_staging_egale_provenance | assert_empreinte_staging_egale_provenance | oui | n/a | dim_etablissement_education, agg_etablissements_par_region_2023, comparaison_versions_par_region | ne_pas_reparer |
| P10 | valeurs_derivees | relationships_stg_education_qualifiant_public_2023_region | relationships_stg_education_qualifiant_public_2023_region__region__ref_dim_region_ | oui | n/a | - | escalader |
