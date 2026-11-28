# Analyse d'impact (J9, A) : dbt `state:modified+` et Marquez

Date : 2026-11-27. Scripts : `scripts/analyse_impact.py`. Rapports détaillés : `data/rapports/impact_*.md`.

## Objectif

Savoir ce qu'un changement ou une panne touche en aval, avec deux sources : le manifest dbt et le graphe
de lineage de Marquez, puis comparer leurs résultats pour trouver les angles morts de chacune.

## Méthode

- **dbt** : `dbt ls` avec une sélection.
  - panne sur une table brute : `--select source:raw.<table>+`
  - modification de code ou de seed : `--select state:modified+ --state /opt/airflow/dbt/state_ref`
  - Le manifest de référence est produit par `dbt parse` sur le code non modifié, puis copié dans
    `dbt/state_ref/` (dossier non versionné, présent dans `.gitignore`).
- **Marquez** : parcours des arcs sortants du graphe (`GET /api/v1/lineage`) à partir du dataset de départ.
- **Comparaison** : modèles en aval selon dbt, datasets en aval selon Marquez, écarts dans les deux sens.
- Les modifications des scénarios 2 et 3 ont été temporaires et annulées avec `git checkout`.
  Aucune donnée n'a été modifiée, `dbt ls` ne construit rien.

## Résultats

| Scénario | Départ | dbt | Marquez | Écart |
|---|---|---|---|---|
| 1. Panne sur une table brute | `lineage.raw.education_qualifiant_public_2023` | 4 modèles, 16 tests | 4 datasets | aucun |
| 2. Modèle modifié | `stg_education_prive_2026` (commentaire ajouté) | 3 modèles dont le départ, 14 tests | 2 datasets | aucun |
| 3. Seed modifié | `dim_region` (ligne ajoutée) | 1 seed, 4 modèles, 26 tests | départ introuvable (HTTP 404) | Marquez ne voit rien |

### Scénario 1 : panne sur `raw.education_qualifiant_public_2023`

- Modèles impactés : `stg_education_qualifiant_public_2023`, `dim_etablissement_education`,
  `agg_etablissements_par_region_2023`, `comparaison_versions_par_region`.
- Marquez retrouve exactement les mêmes 4 datasets.
- Cohérent avec l'injection n°3 de J5 (`NOTES.md`) : les 14 nœuds ignorés étaient ces trois marts et leurs tests.

### Scénario 2 : modification de `stg_education_prive_2026`

- Impactés en aval : `dim_etablissement_education` et `comparaison_versions_par_region`, des deux côtés.
- `agg_etablissements_par_region_2023` n'est pas impacté, ni selon dbt ni selon Marquez : il ne lit que les
  fichiers 2023. L'analyse est précise, elle ne désigne pas tout le graphe.
- Marquez montre aussi les 4 tests singuliers (`assert_*`) sous forme de jobs.

### Scénario 3 : modification du seed `dim_region`

- dbt : `dim_etablissement_education`, `agg_etablissements_par_region_2023`, `agg_hopitaux_par_region`,
  `comparaison_versions_par_region`, et 26 tests.
- Marquez : le dataset `lineage.ref.dim_region` n'existe pas, le départ est introuvable. Interroger Marquez
  seul conduirait à conclure à tort « aucun impact ».
- Les 26 tests comprennent les tests `relationships` des 9 modèles de staging : ces modèles ne sont pas en aval
  du seed, mais leurs tests le référencent.

## Ce que montre la comparaison

- Quand le départ est une table du flux de données (raw, staging, marts), dbt et Marquez concordent
  (scénarios 1 et 2).
- Quand le départ est une dépendance absente de Marquez (le seed `dim_region`), seul le manifest dbt donne
  l'impact (scénario 3). Limite déjà notée en J4 : le seed n'est pas émis dans Marquez. L'hypothèse
  (`openlineage-dbt` ne traite pas les seeds dans cette configuration) n'a pas été vérifiée.
- `state:modified+` détecte un changement de code ou de seed, pas un changement de contenu dans une table
  brute. Pour une panne de données, la sélection est `source:raw.<table>+`.

## Limites

- Les tests ne sont pas comparés un par un : dbt compte les tests individuels, Marquez les regroupe par job
  de modèle et affiche les tests singuliers comme jobs `dbt-run-lineage_etl.test.…` (classés « autres » par le script).
- L'analyse donne un périmètre, pas une gravité ni l'état courant des modèles concernés.
- Une seule base, runs manuels, trois scénarios. Le cas d'une macro modifiée (`normalize_region`) n'a pas été testé.

## Proposition pour la suite (à valider avec B)

Pour l'analyse d'impact du réparateur : partir du manifest dbt pour obtenir la liste complète des nœuds
impactés, et utiliser Marquez pour l'état des jobs et des runs (dernier échec, message d'erreur).
Si le départ est absent de Marquez, s'appuyer sur dbt seul.

## Reproduire

```powershell
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
```