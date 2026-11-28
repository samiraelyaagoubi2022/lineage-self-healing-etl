# Analyse d'impact (J9, A) : dbt `state:modified+` et Marquez

Date : 2026-11-27, mise à jour le 2026-11-28 (ajout de l'état des jobs).
Script : `scripts/analyse_impact.py`. Rapports détaillés : `data/rapports/impact_*.md`.

## Objectif

Savoir ce qu'un changement ou une panne touche en aval, avec deux sources : le manifest dbt et Marquez.
dbt donne le périmètre, Marquez donne l'état des jobs et des runs. On compare aussi leurs périmètres pour
trouver les angles morts de chacun.

## Méthode

- **Périmètre dbt** : `dbt ls` avec une sélection.
  - panne sur une table brute : `--select source:raw.<table>+`
  - modification de code ou de seed : `--select state:modified+ --state /opt/airflow/dbt/state_ref`
  - Le manifest de référence est produit par `dbt parse` sur le code non modifié, puis copié dans
    `dbt/state_ref/` (dossier non versionné, présent dans `.gitignore`).
- **Graphe Marquez** : parcours des arcs sortants du graphe (`GET /api/v1/lineage`) à partir du dataset de départ.
- **État des jobs Marquez** : pour chaque job producteur du départ (par exemple le job d'ingestion) et pour le job
  dbt de chaque modèle du périmètre (retrouvé par son nom `….<modèle>.build.run`), état du dernier run
  (`latestRun`) et, s'il est en échec, message d'erreur du dernier run en échec (facet `errorMessage`).
  Les jobs dbt sont retrouvés par leur nom : cela fonctionne même si le départ est absent de Marquez.
- **Comparaison** : modèles en aval selon dbt, datasets en aval selon Marquez, écarts dans les deux sens.
- Les modifications des scénarios 2 et 3 ont été temporaires et annulées avec `git checkout`.
  Aucune donnée n'a été modifiée, `dbt ls` ne construit rien.

## Résultats

| Scénario | Départ | dbt | Marquez (graphe) | Écart | État des jobs |
|---|---|---|---|---|---|
| 1. Panne sur une table brute | `lineage.raw.education_qualifiant_public_2023` | 4 modèles, 16 tests | 4 datasets | aucun | tous `COMPLETED` |
| 2. Modèle modifié | `stg_education_prive_2026` (commentaire ajouté) | 3 modèles dont le départ, 14 tests | 2 datasets | aucun | tous `COMPLETED` |
| 3. Seed modifié | `dim_region` (ligne ajoutée) | 1 seed, 4 modèles, 26 tests | départ introuvable (HTTP 404) | Marquez ne voit rien | 4 modèles `COMPLETED`, producteur inconnu |
| 4. Panne sur une table brute | `lineage.raw.sante_hopitaux_2025` | 2 modèles, 7 tests | 2 datasets | aucun | tous `COMPLETED` |

### Scénario 1 : panne sur `raw.education_qualifiant_public_2023`

- Modèles impactés : `stg_education_qualifiant_public_2023`, `dim_etablissement_education`,
  `agg_etablissements_par_region_2023`, `comparaison_versions_par_region`.
- Marquez retrouve exactement les mêmes 4 datasets.
- Producteur du départ : `ingestion_data_gov_ma.charger_fichier.education_qualifiant_public_2023`, `COMPLETED`.
- Cohérent avec l'injection n°3 de J5 (`NOTES.md`) et avec la simulation du changement 1
  (`docs/rapport_changements_simules_j9.md`).

### Scénario 2 : modification de `stg_education_prive_2026`

- Impactés en aval : `dim_etablissement_education` et `comparaison_versions_par_region`, des deux côtés.
- `agg_etablissements_par_region_2023` n'est pas impacté, ni selon dbt ni selon Marquez : il ne lit que les
  fichiers 2023. L'analyse est précise, elle ne désigne pas tout le graphe.
- Le départ est un dataset de staging : son producteur est le job dbt `stg_education_prive_2026.build.run`.
- Marquez montre aussi 7 jobs de tests (3 jobs `.build.test` et 4 tests singuliers).

### Scénario 3 : modification du seed `dim_region`

- dbt : `dim_etablissement_education`, `agg_etablissements_par_region_2023`, `agg_hopitaux_par_region`,
  `comparaison_versions_par_region`, et 26 tests.
- Marquez : le dataset `lineage.ref.dim_region` n'existe pas, le départ est introuvable. Interroger Marquez
  seul conduirait à conclure à tort « aucun impact ». Le producteur du départ est inconnu.
- L'état des jobs reste disponible : les 4 modèles du périmètre dbt sont retrouvés par leur nom dans Marquez,
  tous `COMPLETED`.
- Les 26 tests comprennent les tests `relationships` des 9 modèles de staging : ces modèles ne sont pas en aval
  du seed, mais leurs tests le référencent.

### Scénario 4 : panne sur `raw.sante_hopitaux_2025`

- dbt : `stg_sante_hopitaux_2025` et `agg_hopitaux_par_region`, 7 tests. Marquez : les mêmes 2 datasets.
- Producteur du départ : `ingestion_data_gov_ma.charger_fichier.sante_hopitaux_2025`, `COMPLETED`, fin
  2026-11-27T00:00:24, c'est-à-dire la date « Chargé le » de la fiche de provenance de cette table.

## État des jobs dans Marquez

- Dans les 4 scénarios, aucun job n'est en échec dans l'état courant : le tableau ne montre que des
  `COMPLETED`. L'état vient du dernier run envoyé à Marquez : les simulations (builds sur une copie, sans
  `dbt-ol`) n'émettent rien et ne le modifient pas.
- Le seul échec réel documenté dans Marquez est celui provoqué en J7 (ingestion de `sante_hopitaux_2025` avec une
  URL volontairement fausse) : facet `errorMessage` sur le job d'ingestion de cette table. Il n'a pas été rejoué
  avec le nouvel outil ; le rapport afficherait alors le job producteur en `FAILED` avec son message.
- Les jobs dbt ont leur dernier run du 2026-11-26 vers 16:57 UTC, avant le dernier chargement des tables brutes
  (23:54 à 00:00). Ce décalage pourrait servir de signal de péremption d'un modèle ; il n'est pas exploité.

## Ce que montre la comparaison

- Quand le départ est une table du flux de données (raw, staging, marts), dbt et Marquez concordent
  (scénarios 1, 2 et 4).
- Quand le départ est une dépendance absente de Marquez (le seed `dim_region`), seul le manifest dbt donne le
  périmètre (scénario 3). Limite déjà notée en J4 : le seed n'est pas émis dans Marquez. L'hypothèse
  (`openlineage-dbt` ne traite pas les seeds dans cette configuration) n'a pas été vérifiée.
- `state:modified+` détecte un changement de code ou de seed, pas un changement de contenu dans une table
  brute. Pour une panne de données, la sélection est `source:raw.<table>+`.

## Limites

- Les tests ne sont pas comparés un par un : dbt compte les tests individuels, Marquez les regroupe par job
  de modèle et affiche les tests singuliers comme jobs `dbt-run-lineage_etl.test.…`.
- L'analyse donne un périmètre et l'état des derniers runs, pas une gravité.
- Une seule base, runs manuels, quatre scénarios. Le cas d'une macro modifiée (`normalize_region`) n'a pas été testé.

## Méthode retenue pour le réparateur (validée avec B)

Partir du manifest dbt pour obtenir la liste complète des nœuds impactés, et utiliser Marquez pour l'état des
jobs et des runs (dernier échec, message d'erreur). Si le départ est absent de Marquez, s'appuyer sur dbt seul
pour le périmètre, en gardant l'état des jobs dbt retrouvés par leur nom. Cette méthode est implémentée dans
`scripts/analyse_impact.py`.

## Reproduire

powershell
# manifest de référence (code non modifié)
docker compose exec airflow bash -c 'export PATH=/home/airflow/dbt_venv/bin:$PATH && cd /opt/airflow/dbt && dbt parse && mkdir -p state_ref && cp target/manifest.json state_ref/manifest.json'

# 1. panne sur une table brute
Get-Content scripts\analyse_impact.py | docker compose exec -T airflow python - lineage.raw.education_qualifiant_public_2023 --select source:raw.education_qualifiant_public_2023+

# 2. modèle modifié (puis annuler : git checkout -- dbt/models/staging/stg_education_prive_2026.sql)
Add-Content dbt\models\staging\stg_education_prive_2026.sql "-- test impact"
Get-Content scripts\analyse_impact.py | docker compose exec -T airflow python - lineage.staging.stg_education_prive_2026 --select state:modified+ --state /opt/airflow/dbt/state_ref

# 3. seed modifié (puis annuler : git checkout -- dbt/seeds/dim_region.csv)
Add-Content dbt\seeds\dim_region.csv "13,Region test"
Get-Content scripts\analyse_impact.py | docker compose exec -T airflow python - lineage.ref.dim_region --select state:modified+ --state /opt/airflow/dbt/state_ref

# 4. panne sur une autre table brute
Get-Content scripts\analyse_impact.py | docker compose exec -T airflow python - lineage.raw.sante_hopitaux_2025 --select source:raw.sante_hopitaux_2025+
