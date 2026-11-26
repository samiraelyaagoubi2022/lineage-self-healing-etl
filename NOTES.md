# lineage-self-healing-etl

Projet DataOps (15 jours, binôme A/B) : un pipeline ETL sur des données ouvertes marocaines (data.gov.ma) qui **se répare automatiquement** quand il tombe en panne, guidé par le graphe de lineage.

- **Orchestration** : Airflow 2.10.5
- **Transformation** : dbt (dbt-core 1.9.4, dbt-duckdb 1.9.4)
- **Stockage** : DuckDB (un fichier, `lineage.duckdb`)
- **Lineage** : OpenLineage + Marquez 0.50.0
- **Plus tard** : banc de pannes, analyse d'impact, réparateur à règles, validateur

Ce fichier explique comment installer le projet, ce qui existe déjà, et comment travailler dessus. Les détails d'environnement et les problèmes rencontrés sont dans `NOTES.md`.

---

## 1. Où en est le projet

| Étape | État |
|---|---|
| J1 : Docker (Marquez + Airflow), lineage testé à la main | Fait |
| J2 : choix des 9 jeux de données, venv dbt, volume DuckDB, RAM mesurée | Fait (`NOTES.md` à compléter) |
| J3 : DAG d'ingestion data.gov.ma vers DuckDB | **Fait, vérifié** : 9 tables, 9 sha256 identiques à `docs/provenance.md` |
| J3 : DAG de séquence `pipeline_education_sante` | Écrit, **pas encore testé** |
| J3 : projet dbt (staging) et test `dbt-ol` vers Marquez | **À faire (B)** |
| J4 : provenance complète (organisme), tests dbt, tables métier | À faire |

La suite et le détail par personne sont dans la checklist (`checklist-sheets`).

## 2. Schéma d'ensemble

```
data.gov.ma (9 fichiers XLSX)
      |  téléchargement
      v
Airflow : DAG ingestion_data_gov_ma
      |  lecture + chargement, sans transformation
      v
DuckDB, schéma raw   (9 tables + raw._provenance)
      |
      v
dbt : staging  ->  marts            (à construire)
      |
      v
OpenLineage -> Marquez (graphe de lineage, états des runs)
```

Marquez reçoit aussi les événements d'Airflow. Le but final est de lire ce graphe pour trouver la cause d'une panne et son périmètre.

## 3. Installation (à faire une fois)

Prérequis : Docker Desktop, Git. Sous Windows, utiliser PowerShell.

```powershell
git clone <url-du-depot>
cd projet-lineage
copy .env.example .env          # ne JAMAIS commiter .env
docker compose up -d --build    # la 1re fois : plusieurs minutes
docker compose ps               # 5 conteneurs attendus
```

Si le build s'arrête sur une erreur `Read timed out` (réseau lent pendant `pip install`), relancez la même commande : les étapes déjà réussies sont en cache.

**Après chaque `git pull` qui modifie le `Dockerfile` ou le `docker-compose.yml`**, refaites `docker compose up -d --build`. Un simple `build` ne remplace pas le conteneur en marche, il faut aussi `up -d`.

### Accès

| Service | Adresse | Remarque |
|---|---|---|
| Marquez (interface) | http://localhost:3000 | choisir le namespace `projet` en haut à droite |
| Marquez (API) | http://localhost:5000 | |
| Marquez (santé) | http://localhost:5001/healthcheck | |
| Airflow | http://localhost:8080 | compte `admin` / `admin` (utiliser `localhost`, pas `0.0.0.0`) |

Utilisation locale uniquement : Airflow 2.10 n'est plus maintenu, ne jamais exposer le port 8080 sur Internet.

## 4. Structure du dépôt

```
projet-lineage/
|-- docker-compose.yml        5 conteneurs + volume DuckDB
|-- airflow/Dockerfile        image Airflow + duckdb + openpyxl + venv dbt
|-- dags/
|   |-- ingestion_data_gov_ma.py      ingestion des 9 fichiers (testé)
|   `-- pipeline_education_sante.py   ingestion puis dbt (pas encore testé)
|-- dbt/                      projet dbt (à créer par B)
|-- data/raw/                 XLSX téléchargés (NON versionné)
|-- scripts/                  scripts d'exploration des données (explore_ckan.py, ...)
|-- docs/                     provenance.md, notes de conception
|-- NOTES.md                  environnement, versions, problèmes rencontrés
`-- .env.example              variables à copier dans .env
```

Les dossiers `dags/`, `data/`, `dbt/` sont montés dans le conteneur Airflow : un fichier modifié sur votre PC est vu tout de suite dans `/opt/airflow/...`.

## 5. Les données

9 fichiers XLSX de data.gov.ma. La liste exacte (URL, date, sha256) est dans `docs/provenance.md`.

| Table brute (schéma `raw`) | Fichier source | Version |
|---|---|---|
| `education_primaire_public_2023` | établissements publics primaire | 2023 |
| `education_primaire_public_2026` | établissements publics primaire | 2026 |
| `education_college_public_2023` | établissements publics collégial | 2023 |
| `education_college_public_2026` | établissements publics collégial | 2026 |
| `education_qualifiant_public_2023` | établissements publics qualifiants | 2023 |
| `education_qualifiant_public_2026` | établissements publics qualifiants | 2026 |
| `education_prive_2023` | établissements privés | 2023 |
| `education_prive_2026` | établissements privés | 2026 |
| `sante_hopitaux_2025` | liste des hôpitaux | 2025 |

Attention : le jeu `liste-des-etablissements-prives-2023` contient aussi une version 2026. Le nom du jeu ne donne donc pas l'année des données. L'année qui fait foi est celle de la colonne `_version`.

### Dérive de schéma (important)

Les listes scolaires existent en deux schémas :

| Version | Colonnes géographiques |
|---|---|
| 2023 | `REGION`, `PROVINCE`, `COMMUNE` |
| 2026 | `LL_REG`, `LL_PROV`, `LL_COM` |

Le reste des colonnes du primaire est identique (`NOM_ETABL`, `NOM_ETABA`, `AdresseL`, `AdresseA`). **L'ingestion ne renomme rien** : cette différence est volontairement conservée, car elle fournit de vraies pannes de schéma au banc de pannes. C'est à dbt (staging) de les ramener à un schéma commun. Vérifiez les colonnes des autres jeux avec `describe` (voir section 7).

Dans les libellés, `COMMUNE` contient des valeurs comme `Agdal Riyad (Arron...)` ou `Témara (Mun.)` : la normalisation des noms de régions et de communes ne sera pas triviale.

## 6. Conventions de nommage

| Couche | Rôle | Nommage |
|---|---|---|
| `raw` | fichier source tel quel | `domaine_niveau_statut_annee`, ex. `education_primaire_public_2023` |
| `staging` | une table par table brute, colonnes renommées et typées | préfixe `stg_` |
| `marts` | tables métier | `dim_...` (référentiel) ou `agg_...` (agrégat) |

Règles : minuscules, `snake_case`, sans accents, en français, une version = un suffixe d'année.

Dans `raw`, les colonnes d'origine ne sont **pas** renommées. Les colonnes ajoutées par l'ingestion sont préfixées par `_` : `_feuille`, `_jeu`, `_version`, `_source_url`, `_sha256`, `_charge_le`, `_run_id`. Une colonne qui commence par `_` n'est donc jamais une colonne du fichier d'origine.

DAGs : `ingestion_data_gov_ma`, `pipeline_education_sante`.

Cette convention est proposée par A : B la confirme ou l'ajuste avant d'écrire le staging, et la version finale se note dans `NOTES.md`.

## 7. Lancer l'ingestion et regarder les données

**Chaque personne a sa propre base DuckDB.** Elle est dans un volume Docker local (`duckdb-data`) et n'est pas dans Git. Après l'installation, votre base est vide : il faut lancer l'ingestion une fois.

### Lancer le DAG

1. Ouvrez http://localhost:8080 et connectez-vous.
2. Sur la ligne `ingestion_data_gov_ma`, cliquez sur le bouton **▶** (colonne Actions), puis **Trigger DAG w/ config**.
3. Dans le champ `tables`, mettez **un nom par ligne**, sans crochets ni guillemets :

```
education_primaire_public_2023
education_primaire_public_2026
education_college_public_2023
education_college_public_2026
education_qualifiant_public_2023
education_qualifiant_public_2026
education_prive_2023
education_prive_2026
sante_hopitaux_2025
```

4. Cliquez sur **Trigger**, puis ouvrez l'onglet **Grid**. 9 tâches `charger_fichier` s'exécutent **une par une** (quelques minutes selon le réseau).

Pour tester un seul fichier, ne mettez qu'un nom dans le champ.

### Vérifier le résultat

Utilisez des guillemets simples autour de la requête SQL et `read_only=True` :

```powershell
# Les 9 tables et leur provenance
docker compose exec airflow python -c "import duckdb; c=duckdb.connect('/opt/airflow/warehouse/lineage.duckdb', read_only=True); c.sql('select table_brute, version, nb_lignes, left(sha256,12) as sha from raw._provenance order by 1').show()"

# Voir quelques lignes
docker compose exec airflow python -c "import duckdb; c=duckdb.connect('/opt/airflow/warehouse/lineage.duckdb', read_only=True); c.sql('select * from raw.education_primaire_public_2023 limit 3').show(max_width=250)"

# Voir les colonnes d'une table
docker compose exec airflow python -c "import duckdb; c=duckdb.connect('/opt/airflow/warehouse/lineage.duckdb', read_only=True); c.sql('describe raw.education_primaire_public_2026').show(max_rows=30)"
```

Résultats attendus (sha256, 12 premiers caractères) :

| Table | Lignes | sha256 |
|---|---|---|
| education_primaire_public_2023 | 7 825 | `fc32a4b01d81` |
| education_primaire_public_2026 | 7 963 | `e3967f57b114` |
| education_college_public_2023 | 2 159 | `6bbc8cf18d85` |
| education_college_public_2026 | 2 401 | `a56d7d382fb2` |
| education_qualifiant_public_2023 | 1 414 | `be19f6899115` |
| education_qualifiant_public_2026 | 1 653 | `05b7d68815ab` |
| education_prive_2023 | 5 684 | `4eea5565c83c` |
| education_prive_2026 | 6 387 | `1f535cb1daa8` |
| sante_hopitaux_2025 | 184 | `4f1f5645b2f8` |

Si un sha256 est différent, le fichier a changé sur data.gov.ma depuis le téléchargement de référence (2026-11-18).

## 8. Ce que fait le DAG d'ingestion

Pour chacun des 9 fichiers (une tâche Airflow par fichier, grâce au mapping dynamique) :

1. **Télécharge** le XLSX avec un `User-Agent` de navigateur, et vérifie que c'est bien un XLSX et non une page d'erreur.
2. **Calcule le sha256** du fichier (son empreinte).
3. **Lit toutes les feuilles** en texte, sans rien interpréter, et détecte la ligne d'en-tête (elle n'est pas toujours en ligne 0) en cherchant `region`, `ll_reg` ou `nom_etabl`.
4. **Ajoute les colonnes de traçabilité** (`_jeu`, `_version`, `_source_url`, `_sha256`, `_charge_le`, `_run_id`, `_feuille`).
5. **Écrit dans DuckDB** avec `CREATE OR REPLACE TABLE raw.<table>` : le rejouer remplace la table, sans doublon. Il écrit aussi une ligne dans `raw._provenance`.

Les erreurs volontaires (`AirflowFailException`) ne sont pas réessayées : fichier qui n'est pas un XLSX, feuille sans en-tête reconnu, aucune table qui correspond au filtre. Les autres erreurs (réseau) sont réessayées 2 fois, avec 30 secondes d'attente.

## 9. Guide pour B : créer le projet dbt

Le dossier `dbt/` ne contient que `.gitkeep`. Les fichiers à créer :

**`dbt/dbt_project.yml`**
```yaml
name: projet
version: "1.0.0"
config-version: 2
profile: projet

model-paths: ["models"]
test-paths: ["tests"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  projet:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

**`dbt/profiles.yml`**
```yaml
projet:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /opt/airflow/warehouse/lineage.duckdb
      threads: 1
```
`DBT_PROFILES_DIR` pointe déjà sur `/opt/airflow/dbt` (défini dans `docker-compose.yml`).

**Sources** : déclarez le schéma `raw` et les 9 tables (`dbt/models/staging/sources.yml`), avec `schema: raw`, puis écrivez les modèles avec `{{ source('raw', 'education_primaire_public_2023') }}`. Pensez aussi à `raw._provenance`.

**Noms de schémas** : par défaut, dbt crée des schémas du type `main_staging`. Pour obtenir `staging` et `marts`, ajoutez la macro `generate_schema_name` dans `dbt/macros/`. À valider avec A.

**Staging** : une table `stg_...` par table brute, qui ramène `REGION` et `LL_REG` (idem province et commune) vers des colonnes communes, normalise les libellés et convertit les types (tout est en `VARCHAR` dans `raw`).

**Lancer dbt** (le venv dbt n'est pas dans le PATH global, il faut le placer en tête pour la commande) :

```powershell
docker compose exec airflow bash -c 'export PATH=/home/airflow/dbt_venv/bin:$PATH && cd /opt/airflow/dbt && dbt debug'
docker compose exec airflow bash -c 'export PATH=/home/airflow/dbt_venv/bin:$PATH && cd /opt/airflow/dbt && dbt build'
```

Pour envoyer aussi les événements à Marquez, remplacez `dbt build` par `dbt-ol build`, puis regardez le graphe dans Marquez (namespace `projet`). Les variables `OPENLINEAGE_URL` et `OPENLINEAGE_NAMESPACE` sont déjà dans `docker-compose.yml`.

Ajoutez dans `.gitignore` : `dbt/target/`, `dbt/logs/`, `dbt/dbt_packages/`.

## 10. Règles à respecter

1. **DuckDB n'a qu'un seul écrivain.** Ne lancez jamais l'ingestion et dbt en même temps, et n'ouvrez pas la base en écriture pendant un run. Les commandes de lecture ci-dessus utilisent `read_only=True` et se lancent entre deux runs.
2. **Un dépôt, une branche par tâche** (ex. `j3-dag-ingestion`), une Pull Request relue par l'autre avant la fusion. Le relecteur doit comprendre le code avant d'approuver.
3. **Ne jamais commiter** `.env`, `data/raw/`, ni de fichier `*.duckdb`. Avant un `git add`, vérifiez avec `git status`.
4. **Un responsable par tâche** dans la checklist, l'autre vérifie (colonne « Vérifié par l'autre »). On ne coche pas sa propre vérification.
5. **Rôles tournants** : chacun doit toucher à Airflow, dbt et OpenLineage/Marquez. Mini-présentation de 20 minutes à la fin de chaque module.
6. Quand l'un est bloqué plus de 30 à 45 minutes, passez en programmation à deux (un écrit, l'autre guide), puis échangez.
7. Notez dans `NOTES.md` ce que vous apprenez et les erreurs rencontrées : c'est la matière du rapport.

## 11. Problèmes déjà rencontrés

| Symptôme | Cause | Solution |
|---|---|---|
| `dbt-ol: command not found` | Image non reconstruite, ou conteneur non recréé | `docker compose up -d --build` |
| `Read timed out` pendant le build | Réseau lent pendant `pip install` | Relancer la même commande de build |
| `$PATH` vide dans une commande | PowerShell remplace `$PATH` entre guillemets doubles | Mettre la commande entre guillemets simples |
| `ERR_ADDRESS_INVALID` sur le navigateur | Adresse `0.0.0.0:8080` | Utiliser `localhost:8080` |
| `Aucune table ne correspond a ['[...]']` | Champ `tables` saisi avec des crochets | Un nom par ligne, sans crochets ni guillemets |
| `aucune feuille avec une colonne region` | En-tête 2026 (`LL_REG`) non reconnu par une ancienne version du DAG | Utiliser la version actuelle du DAG |
| `Connection has already been closed` (test DuckDB) | Connexion non gardée dans une variable | `c=duckdb.connect(...); c.sql(...).show()` |
| Marquez répond 400 `Invalid UTF-8` | PowerShell 5.1 n'envoie pas l'UTF-8 | Envoyer les octets UTF-8 explicitement |
| Erreur de verrou DuckDB | Un autre processus écrit dans la base | Attendre la fin du run, ne pas lancer deux écritures |
| Tâche `charger_fichier` rouge | Voir le log : onglet Grid, clic sur la tâche, bouton Log | Lire le message, il nomme la table |

Pour lire un log en ligne de commande (remplacer `1` par l'index de la tâche) :

```powershell
docker compose exec airflow bash -c "tail -n 25 /opt/airflow/logs/dag_id=ingestion_data_gov_ma/run_id=*/task_id=charger_fichier/map_index=1/attempt=1.log"
```

## 12. Commandes utiles

```powershell
docker compose ps                              # état des conteneurs
docker compose logs --tail 50 airflow          # logs d'un service
docker compose down                            # arrêter (garde les données)
docker compose down -v                         # TOUT effacer, y compris DuckDB et les bases
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-import-errors
docker stats --no-stream                       # RAM utilisée
```

`docker compose down -v` supprime le volume DuckDB : il faudra relancer l'ingestion.

## 13. À faire et limites connues

- L'organisme (CKAN) n'est pas encore dans `raw._provenance` (prévu en J4).
- Le DAG retélécharge les 9 fichiers à chaque run, et enregistre le sha256 sans le comparer à l'exécution précédente.
- Aucun contrôle de qualité sur les données : c'est le rôle des tests dbt.
- Une erreur en cours de route peut laisser une partie des tables chargée.
- `pipeline_education_sante` n'a pas encore été exécuté.
- La licence des données data.gov.ma est **à vérifier** avant de rendre le dépôt public.
- Migration vers Airflow 3 : perspective (l'API REST et l'authentification changent).

## Provenance (J4)

`raw._provenance` contient une ligne par table brute :
- URL du fichier, empreinte sha256, date de chargement, run Airflow, nombre de lignes ;
- depuis le catalogue CKAN de data.gov.ma : organisme, licence et date de modification du jeu.

Relevé du 2026-11-23 (9 tables, les quatre colonnes remplies pour chacune) :
- organisme : MENPS pour les 8 jeux d'éducation, MSPS pour les hôpitaux (titres des organisations sur le portail) ;
- licence déclarée par le catalogue : Open Data Commons Open Database License (ODbL) pour les 9 jeux.

À faire : confirmer la licence sur la page « Licence » du portail avant de publier le dépôt
(l'ODbL prévoit une attribution et un partage à l'identique : à vérifier pour un dépôt public).

Si le catalogue ne répond pas, ces champs restent vides et le log de la tâche affiche « ATTENTION » :
l'ingestion n'échoue pas pour autant.




### Lineage dbt dans Marquez (relevé le 2026-11-21, mis à jour le 2026-11-23)
- Datasets : namespace `duckdb:///opt/airflow/warehouse/lineage.duckdb`, noms `lineage.raw.<table>`, `lineage.staging.stg_<table>` et `lineage.marts.<table>`.
- Jobs dbt : namespace `projet`, un job `.build.run` et un job `.build.test` par modèle (`dbt-run-lineage_etl.lineage.<schéma>.lineage_etl.<modèle>.build.run`), plus `dbt-run-lineage_etl`.
- Jobs Airflow (`ingestion_data_gov_ma`, `charger_fichier`, `choisir_sources`) : aucun dataset déclaré, donc pas de lien automatique avec les tables raw. À traiter à J8.
- dbt-ol : 20 événements pour 9 modèles (J3) ; 46 événements après J4 (seed et tests inclus).
- Écart manifest / Marquez (check_manifest_vs_marquez.py, 2026-11-23) : les jobs `agg_etablissements_par_region_2023` et `agg_hopitaux_par_region` n'ont pas `lineage.ref.dim_region` en entrée dans Marquez, alors que le manifest dbt la déclare (`dbt ls --select dim_region+` retourne bien les 2 modèles). Le seed n'a ni dataset ni job dans Marquez (hypothèse : seeds non traités par openlineage-dbt 1.55.0 dans cette configuration, non vérifiée). Conséquence : l'analyse d'impact doit croiser le manifest dbt et Marquez. Pistes pour plus tard : événement OpenLineage complémentaire émis par notre code, ou remplacer le seed par un modèle (non testées).

### Résultats J4 (B, 2026-11-23)
- dbt-ol build : PASS=40 WARN=7 ERROR=0 SKIP=0 (47 éléments : 1 seed, 11 modèles, 35 tests).
- 6 avertissements liés à « Casablanca-Settat », absente de dim_region (qui suit les noms de 2023) : 4 tests relationships éducation 2026 (437 + 1 206 + 1 654 + 301 = 3 598 lignes), 1 test hôpitaux (27 lignes), 1 région sans code dans agg_hopitaux_par_region. Cas R2 : dérive silencieuse détectée par un test.
- 1 avertissement not_null etablissement : 2 établissements sans nom dans primaire 2023 (absent en 2026).
- Les 4 tests relationships sur les modèles 2023 passent : les noms 2023 correspondent à 100 % au référentiel.
- Pas de clé naturelle sur les établissements (17 082 lignes, 15 876 noms distincts, 17 069 couples nom/commune distincts, calcul qui ignore les lignes à nom ou commune vide) : pas de test `unique` sur le nom.
- Décisions proposées, à valider avec A : tests 2023 en error, tests 2026 et hôpitaux en warn ; not_null etablissement en warn (anomalie de la source, non corrigée en staging) ; agg_etablissements_par_region_2023 séparé de la santé, car une jointure sur la région perdrait Casablanca en silence.

### Versions fixées (conteneur Airflow, relevées le 2026-11-22)
- Airflow 2.10.5
- apache-airflow-providers-openlineage 2.0.0, openpyxl 3.1.5 (fixées dans le Dockerfile)
- dbt-core 1.9.4, dbt-duckdb 1.9.4, duckdb 1.2.2 (fixées)
- dbt-adapters 1.16.3, dbt-common 1.27.1 (fixées, dépendances de dbt)
- openlineage-dbt 1.55.0 (fixée)
- Python 3.12.9 dans le venv /home/airflow/dbt_venv
- Démarrage d'Airflow : `rm -f /opt/airflow/airflow-webserver.pid` avant le serveur web
  (évite un arrêt du conteneur dû à un fichier pid périmé après un arrêt brutal)



## Contenu d'un échec dans Marquez (J5)
- Run du job Airflow `dbt_build` : état FAILED, facet `errorMessage` avec le message générique `task_failure` (pas de stackTrace), plus `airflowState` et `airflowDagRun`.
- Run du modèle dbt (`...stg_education_college_public_2023.build.run`) : état FAILED, facets `dbt_run`, `dbt_version`, `parent`, sans message d'erreur.
- Le message d'erreur SQL réel (ex. colonne introuvable) n'est PAS dans Marquez. Il se trouve dans le log de la tâche Airflow et dans `warehouse/failures.jsonl` (champ `echecs_dbt`), alimenté par le callback.
- Conséquence : pour diagnostiquer, partir du run FAILED dans Marquez, puis suivre `log_url` / `failures.jsonl`.


## DAG dbt_build : paramètres (J5)
- `select` est validé par un pattern (`^[A-Za-z0-9_+@,. ]*$`) : une valeur comme `a; ls` est refusée par Airflow avant la création du run (testé en CLI).
- `echec_volontaire` fait `exit 1` avant dbt : le callback se déclenche, mais Marquez ne reçoit aucun événement dbt, seulement l'échec du job Airflow.

## Fichiers non versionnés (J5)
- `.env` : retiré de l'index (jamais de secret dans Git). Modèle : `.env.example`.
- `data/raw/*.xlsx` : retirés de l'index, régénérés par le DAG d'ingestion (source : data.gov.ma). Les anciens commits les contiennent encore (données ouvertes, sans secret).
- 2 lignes de `primaire_public_2023` ont un `etablissement` vide (Es-semara, Gzenaya Al Janoubia) : conservées, test `not_null` en `warn`.

## Fiches de provenance (J5)
- Générées par `scripts/gen_provenance.py` à partir de `raw._provenance`, dans `data/provenance/`.
- Le SHA Git d'une fiche est celui du commit au moment de la génération, pas celui du run d'ingestion. Il précède donc le commit qui ajoute la fiche.
- Les fiches doivent être régénérées après chaque ingestion (sha256, date de chargement et run_id changent).

## Stabilisation du DAG dbt_build (J6)
- Pool Airflow `duckdb` (1 slot) utilisé par `dbt_build` et par `charger_fichier` (ingestion) : ils ne s'exécutent jamais en même temps (verrou DuckDB).
- Le pool n'est pas dans Git. Sur une installation neuve : `airflow pools set duckdb 1 "Verrou DuckDB"`.
- `max_active_runs=1`, timeout de tâche 15 min, timeout de run 20 min, `set -euo pipefail`.
- `select` validé par pattern. `echec_volontaire` fait `exit 1` avant dbt : le callback se déclenche, mais Marquez ne reçoit aucun événement dbt.

## Secrets (J6)
- Aucun secret réel dans le dépôt. `docker-compose.yml` ne contient que des identifiants de développement local (admin/admin, mots de passe Postgres par défaut, clé Flask locale), à ne jamais réutiliser hors machine de dev.
- `.env` : retiré de l'index (jamais de secret dans Git). Modèle : `.env.example`.