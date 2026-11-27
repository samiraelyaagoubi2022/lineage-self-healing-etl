"""Verifie une fiche de provenance jusqu'au fichier source (lecture seule, ne modifie rien).

Pour chaque table : relit la fiche data/provenance/<table>.md, puis controle
  1. SHA-256 de la fiche = raw._provenance
  2. SHA-256 de la fiche = colonne _sha256 de la table brute (une seule valeur)
  3. lignes de la fiche = lignes de raw._provenance = lignes de la table brute
  4. SHA-256 de la fiche = SHA-256 du staging (colonne sha256_source)
  5. l'URL de la fiche = l'URL de raw._provenance, et le fichier telecharge maintenant
     depuis cette URL a la meme empreinte (le portail n'a pas change le fichier)

Lancement dans le conteneur (PowerShell), tables en arguments (defaut : les deux tables verifiees en J8) :
    Get-Content scripts\\verifier_fiche.py | docker compose exec -T airflow python - education_prive_2026 sante_hopitaux_2025
"""
import hashlib
import re
import sys

import duckdb
import requests

DB = "/opt/airflow/warehouse/lineage.duckdb"
FICHES = "/opt/airflow/data/provenance"
HEADERS = {"User-Agent": "Mozilla/5.0"}
TABLES = sys.argv[1:] or ["education_prive_2026", "sante_hopitaux_2025"]


def lire_fiche(table):
    with open(f"{FICHES}/{table}.md", encoding="utf-8") as f:
        texte = f.read()

    def champ(motif):
        m = re.search(motif, texte)
        return m.group(1) if m else None

    return {
        "sha": champ(r"\| SHA-256 du fichier \| `([0-9a-f]{64})`"),
        "url": champ(r"\| URL source \(CKAN\) \| (\S+) \|"),
        "lignes": int(champ(r"\| Lignes charg\S+ \| (\d+) \|") or -1),
        "commit": champ(r"\| Commit Git du projet \| `([^`]+)`"),
        "run": champ(r"\| Run Airflow \(ingestion\) \| `([^`]+)`"),
    }


def ligne(ok, texte):
    print(f"   {'OK   ' if ok else 'ECART'} {texte}")
    return ok


con = duckdb.connect(DB, read_only=True)
tout_ok = True

for table in TABLES:
    print(f"\n== {table}")
    fiche = lire_fiche(table)
    print(f"   fiche : commit {fiche['commit']}, run {fiche['run']}")

    prov = con.execute(
        'select sha256, nb_lignes, url from raw."_provenance" where table_brute = ?', [table]
    ).fetchone()
    if prov is None:
        tout_ok &= ligne(False, "table absente de raw._provenance")
        continue
    sha_prov, nb_prov, url_prov = prov

    nb_brut, nb_sha, sha_brut = con.execute(
        f'select count(*), count(distinct "_sha256"), min("_sha256") from raw."{table}"'
    ).fetchone()
    sha_stg = [r[0] for r in con.execute(
        f'select distinct sha256_source from staging."stg_{table}"').fetchall()]

    tout_ok &= ligne(fiche["sha"] == sha_prov, "SHA-256 fiche = raw._provenance")
    tout_ok &= ligne(nb_sha == 1 and sha_brut == fiche["sha"], "SHA-256 fiche = colonne _sha256 de la table brute")
    tout_ok &= ligne(fiche["lignes"] == nb_prov == nb_brut,
                     f"lignes : fiche {fiche['lignes']} / provenance {nb_prov} / table brute {nb_brut}")
    tout_ok &= ligne(sha_stg == [fiche["sha"]], "SHA-256 fiche = staging (sha256_source)")
    tout_ok &= ligne(fiche["url"] == url_prov, "URL fiche = URL de raw._provenance")

    try:
        reponse = requests.get(fiche["url"], headers=HEADERS, timeout=120)
        reponse.raise_for_status()
        sha_portail = hashlib.sha256(reponse.content).hexdigest()
        tout_ok &= ligne(reponse.content.startswith(b"PK") and sha_portail == fiche["sha"],
                         f"fichier du portail re-telecharge, SHA-256 {sha_portail[:12]}... "
                         f"({'identique a la fiche' if sha_portail == fiche['sha'] else 'DIFFERENT : le portail a change le fichier'})")
    except Exception as e:
        tout_ok &= ligne(False, f"telechargement impossible : {str(e)[:120]}")

con.close()
print("\nRESULTAT :", "tout est coherent" if tout_ok else "au moins un ecart, voir ci-dessus")