# Notes du binôme : lineage-self-healing-etl

Projet DataOps (15 jours) : pipeline ETL sur données ouvertes marocaines (Airflow + dbt),
lineage avec OpenLineage + Marquez, et réparation automatique des pannes guidée par le graphe de lineage.

## Démarrage

```powershell
copy .env.example .env          # une seule fois (ne jamais commiter .env)
docker compose up -d --build    # démarrer (la 1re fois : plusieurs minutes)
docker compose ps               # état des 5 conteneurs
docker compose logs --tail 50 airflow        # logs d'un service
docker compose down             # arrêter (garde les données)
docker compose down -v          # tout effacer (supprime les bases)
```

## Accès

| Service | Adresse | Remarque |
|---|---|---|
| Marquez (interface) | http://localhost:3000 | choisir le namespace `projet` en haut à droite |
| Marquez (API) | http://localhost:5000 | |
| Marquez (santé) | http://localhost:5001/healthcheck | |
| Airflow | http://localhost:8080 | compte `admin` / `admin` (ne pas ouvrir 0.0.0.0:8080) |

## Architecture (docker-compose.yml)

5 conteneurs sur un même réseau Docker : `marquez-db`, `marquez-api`, `marquez-web`, `airflow-db`, `airflow`.
Airflow joint Marquez par le nom interne `marquez-api:5000`.
Dossiers montés dans Airflow : `dags/`, `data/`, `dbt/`, `airflow/logs/`.

## Choix techniques

- Airflow 2.10.5 : stable, très documenté, déjà testé. Cette série n'est plus maintenue (fin de support avril 2026) : usage local uniquement, ne jamais exposer le port 8080 sur Internet. Migration vers Airflow 3 = perspective (l'API REST et l'authentification changent).
- Un seul conteneur Airflow (scheduler + serveur web) : suffisant en local, pas une configuration de production.
- dbt sera dans un conteneur séparé (conflits de dépendances avec Airflow).
- API REST d'Airflow : `basic_auth` activée via `AIRFLOW__API__AUTH_BACKENDS` (nécessaire pour que le réparateur pilote Airflow par script).
- Dataset : données ouvertes data.gov.ma (API CKAN). Plan B : Olist si moins de 3 jeux exploitables et liables.

## Tests réalisés au jour 1

1. **Marquez** : interface sur :3000 et healthcheck sur :5001 répondent.
2. **Airflow** : page de connexion, liste de DAG vide (normal).
3. **API Airflow en basic auth** :
   ```powershell
   $cred = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:admin"))
   Invoke-RestMethod -Uri http://localhost:8080/api/v1/dags -Headers @{Authorization="Basic $cred"}
   ```
   Résultat obtenu : `dags {}` et `total_entries 0` (authentification acceptée).
4. **Lineage à la main** : événement OpenLineage `COMPLETE` envoyé à `POST http://localhost:5000/api/v1/lineage`.
   Graphe visible dans Marquez : `source_brute -> job_test -> table_finale` (namespace `projet`).
5. **Panne simulée** : événements `START` puis `FAIL` sur le même `runId`.
   - Le run apparaît avec l'état `FAILED` : `GET /api/v1/namespaces/projet/jobs/job_test/runs`.
   - Le message d'erreur est lisible : `GET /api/v1/jobs/runs/{id}` -> `facets.errorMessage.message`.
   - Les entrées et sorties du job en échec sont conservées (utile pour trouver l'amont et l'aval).

## Problèmes rencontrés et solutions

| Problème | Cause | Solution |
|---|---|---|
| `Dockerfile: no such file or directory` | `airflow/Dockerfile` absent ou mal enregistré (Windows) | Recréé avec `Set-Content -Encoding ascii` |
| `ERR_CONNECTION_REFUSED` sur :8080 | Commande de démarrage : le serveur web démarrait avant la fin de la migration de la base | Commandes mises dans l'ordre : migration, compte admin, scheduler en arrière-plan, serveur web |
| `ERR_ADDRESS_INVALID` | Adresse `0.0.0.0:8080` ouverte dans le navigateur | Utiliser `localhost:8080` |
| Marquez répond 400 `Invalid UTF-8` | PowerShell 5.1 n'envoie pas l'UTF-8 (accents) | Envoyer `[System.Text.Encoding]::UTF8.GetBytes($json)` avec `charset=utf-8` |
| `renommÃ©e` à l'affichage | PowerShell 5.1 relit mal l'UTF-8 | Problème d'affichage seulement ; Python lira correctement |
| `curl` ne marche pas comme prévu | Sous PowerShell, `curl` est un alias | Utiliser `Invoke-RestMethod` |

## À ne pas oublier

- Jour 7 : vérifier ce que l'intégration réelle Airflow/dbt envoie à Marquez en cas d'échec (le message d'erreur est-il aussi détaillé que dans notre test ?). Sinon, lire la cause dans les logs d'Airflow.
- Jour 10 : le réparateur utilise l'API Airflow (basic auth) et l'API Marquez.
- Vérifier la licence des données data.gov.ma avant de publier le dépôt. Ne jamais commiter `.env` ni les CSV.
- Jour 5 et jour 12 : mini-présentations croisées dans le binôme.

## Journal

- Jour 1 (fait seul) : dépôt Git, docker-compose (Marquez + Airflow), tests ci-dessus. Le binôme doit refaire l'installation à partir de ces notes.