# Rapport d'impact sur 3 changements simulés (J9, A)

Date : 2026-11-28. Scripts : `scripts/simuler_changement.py`, `scripts/comparer_impact.py`.
Rapports détaillés : `data/rapports/simulation_1.md`, `simulation_2.md`, `simulation_3.md`.

## Principe

Pour chaque changement : (1) on prédit l'impact avec dbt (`dbt ls`), (2) on lance un vrai `dbt build` sur
une copie de la base (`essai.duckdb`), (3) on compare les deux listes.

- **Prédit** : modèles, seeds et tests retenus par la sélection dbt.
- **Observé** : nœuds dont le statut (ou le nombre de lignes en défaut d'un test) diffère entre le build de
  référence et le build avec le changement. Un nœud ignoré (skipped) après une erreur est impacté.
- **Omis** : nœud observé mais non prédit. C'est le critère de sûreté : il doit être vide.
- Build de référence sur la copie : PASS=51 WARN=9 ERROR=0 SKIP=0 TOTAL=60.
- La vraie base `lineage.duckdb` n'a pas été modifiée. Les changements 2 et 3 (fichiers dbt) ont été annulés
  avec `git checkout`.

## Résultats

| # | Changement simulé | Sélection dbt | Prédit | Observé | Omis | Prédits inchangés | Part touchée |
|---|---|---|---|---|---|---|---|
| 1 | Dérive de schéma : `REGION` renommée `LL_REG` dans `raw.education_qualifiant_public_2023` (copie) | `source:raw.education_qualifiant_public_2023+` | 20 (4 modèles, 16 tests) | 20 (4, 16) | 0 | 0 | 100 % |
| 2 | Modèle modifié : `region` devient `region_nom` dans `stg_education_prive_2026` | `state:modified+` | 17 (3 modèles, 14 tests) | 13 (2, 11) | 0 | 4 | 76 % |
| 3 | Seed modifié : « Grand Casablanca-Settat » devient « Casablanca-Settat » dans `dim_region` | `state:modified+` | 31 (4 modèles, 1 seed, 26 tests) | 24 (3, 0, 21) | 0 | 7 | 77 % |

Bilan des builds (dbt) : changement 1 `PASS=33 WARN=7 ERROR=1 SKIP=19`, changement 2
`PASS=41 WARN=6 ERROR=2 SKIP=11`, changement 3 `PASS=41 WARN=1 ERROR=4 SKIP=14`.

## Changement 1 : dérive de schéma sur une table brute

- `stg_education_qualifiant_public_2023` passe en ERROR (`Binder Error`, colonne `REGION` introuvable).
- Ignorés : `dim_etablissement_education`, `agg_etablissements_par_region_2023`,
  `comparaison_versions_par_region` et les 16 tests concernés.
- `agg_hopitaux_par_region` et les autres modèles ne sont pas touchés.
- **Prédiction exacte** : 20 nœuds prédits, 20 observés, aucun écart. Marquez (rapport d'impact précédent)
  donnait les mêmes 4 modèles. Cohérent avec l'injection n°3 de J5.

## Changement 2 : colonne renommée dans un modèle

- Le modèle `stg_education_prive_2026` se construit (la vue est valide), mais ses deux tests sur la colonne
  `region` passent en ERROR (`Referenced column "region" not found`).
- Conséquence : `dim_etablissement_education` et `comparaison_versions_par_region` sont ignorés, avec leurs tests.
- `agg_etablissements_par_region_2023` se construit normalement : il ne lit que les fichiers 2023.
- **Aucun omis.** 4 prédits inchangés :
  - `stg_education_prive_2026` : c'est le nœud modifié lui-même, son statut ne change pas ;
  - `not_null_stg_education_prive_2026_etablissement` : ne porte pas sur la colonne renommée ;
  - `assert_empreinte_staging_egale_provenance` et `assert_staging_conserve_les_lignes` : n'utilisent pas la colonne
    renommée.
- Les 2 modèles réellement impactés sont exactement les 2 datasets en aval donnés par Marquez.

## Changement 3 : référentiel modifié

- Les 4 tests `relationships` des modèles 2023 (sévérité error) échouent : 381, 1159, 1376 et 253 lignes
  (3169 lignes). `dim_etablissement_education`, `agg_etablissements_par_region_2023` et
  `comparaison_versions_par_region` sont ignorés.
- Effet de bord mesuré : les 5 tests qui étaient en avertissement (2026 et hôpitaux, 3625 lignes au total)
  passent. Renommer la région dans le seed corrige 2026 mais casse 2023 : le problème est déplacé, pas résolu.
- `agg_hopitaux_par_region` garde le statut success, mais son test `not_null_agg_hopitaux_par_region_region_code`
  passe de 1 ligne en défaut à 0 : le contenu de la table a changé. C'est un vrai impact que le statut du modèle
  ne montre pas.
- **Aucun omis.** 7 prédits inchangés :
  - le seed `dim_region` et ses 4 tests : c'est le nœud modifié et ses propres tests, qui passent ;
  - `agg_hopitaux_par_region` : impact réel, visible seulement par son test (voir ci-dessus) ;
  - `not_null_agg_hopitaux_par_region_region` : ne porte pas sur la colonne touchée.
- Marquez ne peut pas suivre ce départ (le seed n'existe pas dans Marquez).

## Conclusion

- **Sûreté** : sur les 3 changements, aucun nœud impacté n'a été oublié par la prédiction dbt (0 omis).
- **Exactitude** : exacte pour le changement 1 (20/20). Pour les changements 2 et 3, la prédiction est un
  périmètre maximal (76 % et 77 % réellement touchés). Les nœuds en trop sont de trois sortes : le nœud modifié
  lui-même, ses propres tests, et les tests qui ne portent pas sur la colonne ou la table modifiée.
- **Marquez** : quand il voit le départ, il donne exactement les modèles réellement impactés (changements 1 et 2),
  mais ne dit rien des tests ni du seed (changement 3).
- **Pour le réparateur** : utiliser dbt pour le périmètre complet et Marquez pour l'état des jobs. Une correction
  dans le référentiel (seed) déplace le problème de 2026 vers 2023 ; une normalisation en aval (macro
  `normalize_region`) est plus sûre.

## Limites

- Trois changements, une seule exécution chacun, sur une copie de la base.
- La mesure repose sur les statuts : un changement silencieux (données modifiées sans changement de statut) n'est
  pas vu. Le contenu d'un modèle impacté n'apparaît que par ses tests (cas de `agg_hopitaux_par_region`).
- Pour se rapprocher de l'exactitude sur les changements 2 et 3 : exclure le nœud modifié de la liste « en aval »
  et, pour une colonne modifiée, ne garder que les tests et modèles qui la référencent. Non implémenté.

## Reproduire

Voir les commandes de `docs/analyse_impact_j9.md` pour la préparation (copie de la base, build de référence
`essais/base.json`). Puis, pour chaque changement N : copie fraîche de `essai.duckdb`,
`simuler_changement.py N`, `dbt build` avec `DUCKDB_PATH` sur la copie (résultat copié en `essais/changeN.json`),
`comparer_impact.py N <sélection dbt>`, et `git checkout` du fichier modifié (changements 2 et 3).
