"""
DAG d'ingestion : data.gov.ma (XLSX) -> DuckDB, schema `raw`.

- Une tache par fichier source (dynamic task mapping), executees une par une
  (DuckDB n'accepte qu'un seul ecrivain a la fois).
- Les donnees sont chargees TELLES QUELLES : colonnes d'origine non renommees
  (REGION en 2023, LL_REG en 2026), toutes en texte. Le typage et le nettoyage
  se font dans dbt (staging).
- Colonnes ajoutees, prefixees par `_` : _jeu, _version, _source_url, _sha256,
  _charge_le, _run_id, _feuille.
- Table de tracabilite : raw._provenance (une ligne par table brute) : URL, empreinte sha256,
  date de chargement, run Airflow, et, depuis le catalogue CKAN, organisme, licence et date de
  modification du jeu (vides si le catalogue ne repond pas : l'ingestion n'echoue pas pour cela).
- J7 : lineage. Les `outlets` d'un @task ne sont pas templates par Airflow, et l'URI d'un Dataset
  Airflow n'est pas decoupee comme le fait dbt. Chaque tache emet donc elle-meme ses evenements
  OpenLineage (START, COMPLETE ou FAIL) vers Marquez, avec la table brute en sortie, sous le meme
  namespace et le meme nom que dbt : duckdb:///opt/airflow/warehouse/lineage.duckdb / lineage.raw.<table>.
  Si Marquez ne repond pas, on le note dans le log : l'ingestion n'echoue pas pour cela.

Lancement : interface Airflow -> ingestion_data_gov_ma -> Trigger DAG w/ config.
Le champ `tables` permet de ne charger que certaines tables (vide = les 9).
"""
import hashlib
import os
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone

import duckdb
import pandas as pd
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

DB_PATH = "/opt/airflow/warehouse/lineage.duckdb"
RAW_DIR = "/opt/airflow/data/raw"
HEADERS = {"User-Agent": "Mozilla/5.0"}
CKAN_API = "https://data.gov.ma/data/api/3/action"

# Lineage : memes valeurs que celles que dbt envoie a Marquez (voir NOTES.md, J7).
OL_NAMESPACE_JOB = os.environ.get("OPENLINEAGE_NAMESPACE", "projet")
OL_NAMESPACE_DATASET = "duckdb:///opt/airflow/warehouse/lineage.duckdb"
OL_PRODUCER = "https://github.com/projet/lineage-self-healing-etl"

# Source : provenance.md. `table` = nom dans le schema raw (domaine_niveau_statut_annee).
SOURCES = [
    {"table": "education_primaire_public_2023", "jeu": "liste-des-etablissements-publics-primaire", "version": "2023",
     "url": "https://data.gov.ma/data/fr/dataset/4de8f7dc-c536-44e8-93a8-3efd2aca2c52/resource/f1ac441c-01e5-4a63-ab3a-e45a930b9a07/download/etab_pub_primaire_juin-2023.xlsx"},
    {"table": "education_primaire_public_2026", "jeu": "liste-des-etablissements-publics-primaire", "version": "2026",
     "url": "https://data.gov.ma/data/fr/dataset/4de8f7dc-c536-44e8-93a8-3efd2aca2c52/resource/f98bdb47-19a8-4f4d-8407-ae08b56305d5/download/etablissements_publics_primaire_juillet-2026.xlsx"},
    {"table": "education_college_public_2023", "jeu": "liste-de-etablissements-publics-collegial", "version": "2023",
     "url": "https://data.gov.ma/data/fr/dataset/93f014c7-2b83-4fe9-b06b-48364bc8e8e8/resource/445627e2-b1ba-48f5-95ec-7e3aa9a1570f/download/etab_pub_collegial_juin-2023.xlsx"},
    {"table": "education_college_public_2026", "jeu": "liste-de-etablissements-publics-collegial", "version": "2026",
     "url": "https://data.gov.ma/data/fr/dataset/93f014c7-2b83-4fe9-b06b-48364bc8e8e8/resource/e13f4f7f-054e-4186-8197-bccf1bb251d2/download/etablissements_publics_collegial_juillet-2026.xlsx"},
    {"table": "education_qualifiant_public_2023", "jeu": "liste-des-etablissements-publics-qualifiants", "version": "2023",
     "url": "https://data.gov.ma/data/fr/dataset/17805f1b-6d88-41d5-ace9-bf0e4007e51f/resource/1e029230-76cd-4d91-995d-d15b859baf48/download/etab_pub_qualifiant_juin-2023.xlsx"},
    {"table": "education_qualifiant_public_2026", "jeu": "liste-des-etablissements-publics-qualifiants", "version": "2026",
     "url": "https://data.gov.ma/data/fr/dataset/17805f1b-6d88-41d5-ace9-bf0e4007e51f/resource/3ce45071-bbb1-49b4-b8b3-00065c3a0f2e/download/etablissement_publics_qualifiant_juillet-2026.xlsx"},
    {"table": "education_prive_2023", "jeu": "liste-des-etablissements-prives-2023", "version": "2023",
     "url": "https://data.gov.ma/data/fr/dataset/63ae019b-1e5c-409a-b53f-0ccac9ddc223/resource/4f6dd560-bad6-4418-b5e8-73ca57811b13/download/etab_priv_juin-2023.xlsx"},
    {"table": "education_prive_2026", "jeu": "liste-des-etablissements-prives-2023", "version": "2026",
     "url": "https://data.gov.ma/data/fr/dataset/63ae019b-1e5c-409a-b53f-0ccac9ddc223/resource/1efc3be5-046e-438a-a6d0-9d9bac2dd492/download/etablissement_prives_juillet-2026.xlsx"},
    {"table": "sante_hopitaux_2025", "jeu": "la-liste-des-hopitaux", "version": "2025",
     "url": "https://data.gov.ma/data/fr/dataset/2932e8a4-272c-4101-80ef-85519de47e7c/resource/17bc72a8-99b0-45ad-af61-10e06c58c4ba/download/liste-des-hopitaux-2025.xlsx"},
]


def _norm(s):
    s = unicodedata.normalize("NFD", str(s).lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


# Noms de colonnes qui signalent la ligne d'en-tete.
# Schema 2023 : REGION ; schema 2026 : LL_REG ; NOM_ETABL existe dans les deux.
MARQUEURS_ENTETE = ("region", "ll_reg", "nom_etabl")


def _ligne_entete(brut):
    """Premiere ligne (sur 10) qui contient un nom de colonne connu : l'en-tete n'est pas toujours en ligne 0."""
    for i in range(min(10, len(brut))):
        cellules = [_norm(v) for v in brut.iloc[i].tolist()]
        if any(m in c for c in cellules for m in MARQUEURS_ENTETE):
            return i
    return None


def _noms_uniques(valeurs):
    """Noms de colonnes d'origine, nettoyes des espaces, sans doublon ni vide."""
    vus, noms = {}, []
    for i, v in enumerate(valeurs):
        n = "" if pd.isna(v) else str(v).strip()
        if not n:
            n = f"colonne_{i}"
        if n in vus:
            vus[n] += 1
            n = f"{n}_{vus[n]}"
        else:
            vus[n] = 1
        noms.append(n)
    return noms


def _metadonnees_ckan(jeu):
    """Organisme, licence et date de modification du jeu, lus dans le catalogue CKAN (package_show).

    En cas d'echec (reseau, jeu introuvable, format inattendu), on renvoie des valeurs vides et on
    l'ecrit dans le log : la provenance est alors incomplete, mais les donnees sont quand meme chargees.
    """
    try:
        r = requests.get(f"{CKAN_API}/package_show", params={"id": jeu}, headers=HEADERS, timeout=30)
        r.raise_for_status()
        res = r.json()["result"]
        return {
            "organisme": (res.get("organization") or {}).get("title"),
            "licence": res.get("license_title") or res.get("license_id"),
            "ckan_modifie_le": res.get("metadata_modified"),
        }
    except Exception as e:
        print(f"ATTENTION {jeu} : metadonnees CKAN indisponibles ({str(e)[:120]}). "
              "organisme, licence et date CKAN resteront vides.")
        return {"organisme": None, "licence": None, "ckan_modifie_le": None}


def _emettre_lineage(etat, table, ol_run_id, message=None):
    """Envoie un evenement OpenLineage (START, COMPLETE ou FAIL) pour le chargement d'une table brute.

    Ne leve jamais d'exception : un probleme de lineage ne doit pas faire echouer l'ingestion.
    """
    try:
        from openlineage.client import OpenLineageClient
        from openlineage.client.event_v2 import Dataset as OLDataset
        from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
        from openlineage.client.facet_v2 import error_message_run, job_type_job

        client = OpenLineageClient.from_environment()   # lit OPENLINEAGE_URL (docker-compose.yml)
        run_facets = {}
        if etat == "FAIL" and message:
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=message[:500], programmingLanguage="python")
        evenement = RunEvent(
            eventType=getattr(RunState, etat),
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=ol_run_id, facets=run_facets),
            job=Job(
                namespace=OL_NAMESPACE_JOB,
                name=f"ingestion_data_gov_ma.charger_fichier.{table}",
                facets={"jobType": job_type_job.JobTypeJobFacet(
                    processingType="BATCH", integration="AIRFLOW", jobType="TASK")},
            ),
            producer=OL_PRODUCER,
            inputs=[],
            outputs=[OLDataset(namespace=OL_NAMESPACE_DATASET, name=f"lineage.raw.{table}")],
        )
        client.emit(evenement)
    except Exception as e:
        print(f"ATTENTION {table} : evenement OpenLineage {etat} non envoye ({str(e)[:150]}). "
              "Le lineage Airflow sera incomplet, les donnees sont chargees normalement.")


def _charger(source, run_id):
    """Telecharge, lit et ecrit une table brute dans DuckDB. Renvoie le nombre de lignes."""
    table = source["table"]

    # 1) Telechargement
    os.makedirs(RAW_DIR, exist_ok=True)
    chemin = os.path.join(RAW_DIR, f"{table}.xlsx")
    resp = requests.get(source["url"], headers=HEADERS, timeout=120)
    resp.raise_for_status()
    if not resp.content.startswith(b"PK"):
        raise AirflowFailException(f"{table} : la reponse n'est pas un fichier XLSX (page HTML ?)")
    with open(chemin, "wb") as f:
        f.write(resp.content)
    sha = hashlib.sha256(resp.content).hexdigest()
    meta = _metadonnees_ckan(source["jeu"])

    # 2) Lecture : toutes les feuilles, tout en texte, en-tete detecte
    feuilles = pd.read_excel(chemin, sheet_name=None, header=None, dtype=str, engine="openpyxl")
    morceaux = []
    for nom, brut in feuilles.items():
        h = _ligne_entete(brut)
        if h is None:
            print(f"{table} : feuille '{nom}' ignoree (pas de colonne region)")
            continue
        d = brut.iloc[h + 1:].copy()
        d.columns = _noms_uniques(brut.iloc[h].tolist())
        d = d.dropna(how="all")
        vide = d.isna().all() & d.columns.str.startswith("colonne_")
        d = d.loc[:, ~vide]
        d["_feuille"] = nom
        morceaux.append(d)
    if not morceaux:
        raise AirflowFailException(f"{table} : aucune feuille avec une colonne region. Feuilles : {list(feuilles)}")
    df = pd.concat(morceaux, ignore_index=True)

    # 3) Colonnes de tracabilite
    maintenant = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["_jeu"] = source["jeu"]
    df["_version"] = source["version"]
    df["_source_url"] = source["url"]
    df["_sha256"] = sha
    df["_charge_le"] = maintenant
    df["_run_id"] = run_id

    # 4) Ecriture dans DuckDB (idempotent : on remplace la table)
    con = duckdb.connect(DB_PATH)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.register("df_tmp", df)
        con.execute(f'CREATE OR REPLACE TABLE raw."{table}" AS SELECT * FROM df_tmp')
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw."_provenance" (
                table_brute VARCHAR, jeu VARCHAR, version VARCHAR, url VARCHAR,
                sha256 VARCHAR, charge_le VARCHAR, run_id VARCHAR, nb_lignes BIGINT,
                organisme VARCHAR, licence VARCHAR, ckan_modifie_le VARCHAR)
        """)
        # Migration : une base creee avant J4 n'a pas ces trois colonnes.
        for colonne in ("organisme", "licence", "ckan_modifie_le"):
            con.execute(f'ALTER TABLE raw."_provenance" ADD COLUMN IF NOT EXISTS {colonne} VARCHAR')
        con.execute('DELETE FROM raw."_provenance" WHERE table_brute = ?', [table])
        con.execute("""
            INSERT INTO raw."_provenance"
                (table_brute, jeu, version, url, sha256, charge_le, run_id, nb_lignes,
                 organisme, licence, ckan_modifie_le)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [table, source["jeu"], source["version"], source["url"], sha, maintenant, run_id,
              len(df), meta["organisme"], meta["licence"], meta["ckan_modifie_le"]])
    finally:
        con.close()

    print(f"{table} : {len(df)} lignes, {len(df.columns)} colonnes, sha256 {sha[:12]}")
    return len(df)


@dag(
    dag_id="ingestion_data_gov_ma",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    params={"tables": Param([], type="array", items={"type": "string"},
                            description="Noms de tables a charger (vide = les 9)")},
    tags=["ingestion", "data.gov.ma"],
)
def ingestion_data_gov_ma():

    @task
    def choisir_sources(**context):
        filtre = context["params"].get("tables") or []
        choix = [s for s in SOURCES if not filtre or s["table"] in filtre]
        if not choix:
            raise AirflowFailException(f"Aucune table ne correspond a {filtre}")
        return choix

    # Un seul fichier a la fois : DuckDB n'a qu'un ecrivain.
    # Pas d'`outlets` : le lineage est emis a la main (voir _emettre_lineage).
    @task(max_active_tis_per_dag=1, pool="duckdb")
    def charger_fichier(source, **context):
        table = source["table"]
        ol_run_id = str(uuid.uuid4())   # un identifiant par tentative
        _emettre_lineage("START", table, ol_run_id)
        try:
            nb = _charger(source, context["run_id"])
        except Exception as e:
            _emettre_lineage("FAIL", table, ol_run_id, message=f"{type(e).__name__}: {e}")
            raise
        _emettre_lineage("COMPLETE", table, ol_run_id)
        return {"table": table, "lignes": nb}

    charger_fichier.expand(source=choisir_sources())


ingestion_data_gov_ma()