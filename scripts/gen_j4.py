import pathlib

DBT = pathlib.Path("dbt")
SEEDS = DBT / "seeds"
STG = DBT / "models" / "staging"
MARTS = DBT / "models" / "marts"
for d in (SEEDS, MARTS):
    d.mkdir(parents=True, exist_ok=True)


def write(path, text):
    path.write_text(text, encoding="utf-8")
    print("écrit", path)


# 1) Référentiel des 12 régions (orthographe 2023)
REGIONS = [
    "Béni Mellal-Khénifra", "Drâa-Tafilalet", "Eddakhla-Oued Eddahab",
    "Fès-Meknès", "Grand Casablanca-Settat", "Guelmim-Oued Noun",
    "Laayoune-Sakia El Hamra", "Marrakech-Safi", "Oriental",
    "Rabat-Salé-Kénitra", "Souss-Massa", "Tanger-Tetouan-Al Hoceima",
]
write(SEEDS / "dim_region.csv",
      "region_code,region\n" + "\n".join(f"{i},{r}" for i, r in enumerate(REGIONS, 1)) + "\n")

write(SEEDS / "schema.yml", """version: 2

seeds:
  - name: dim_region
    columns:
      - name: region_code
        data_tests: [unique, not_null]
      - name: region
        data_tests: [unique, not_null]
""")

# 2) Le seed va dans le schéma "ref" (ajout à dbt_project.yml s'il manque)
proj = DBT / "dbt_project.yml"
txt = proj.read_text(encoding="utf-8")
if "\nseeds:" not in txt:
    proj.write_text(txt.rstrip() + "\n\nseeds:\n  lineage_etl:\n    +schema: ref\n", encoding="utf-8")
    print("seeds ajouté à dbt_project.yml")
# 3) Tests du staging : erreur sur la base 2023, avertissement sur 2026 et hôpitaux
NIVEAUX = ("primaire_public", "college_public", "qualifiant_public", "prive")
modeles = []
for n in NIVEAUX:
    for a in ("2023", "2026"):
        modeles.append((f"stg_education_{n}_{a}", "error" if a == "2023" else "warn"))
modeles.append(("stg_sante_hopitaux_2025", "warn"))

yml = "version: 2\n\nmodels:\n"
for nom, sev in modeles:
    yml += f"""  - name: {nom}
    columns:
      - name: etablissement
        data_tests:
          - not_null:
              config:
                severity: warn
      - name: region
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_region')
              field: region
              config:
                severity: {sev}
"""
write(STG / "schema.yml", yml)

# 4) Tables métier
write(MARTS / "agg_etablissements_par_region_2023.sql", """with etab as (
    select * from {{ ref('stg_education_primaire_public_2023') }}
    union all
    select * from {{ ref('stg_education_college_public_2023') }}
    union all
    select * from {{ ref('stg_education_qualifiant_public_2023') }}
    union all
    select * from {{ ref('stg_education_prive_2023') }}
)
select
    r.region_code,
    e.region,
    e.niveau,
    count(*) as nb_etablissements
from etab e
left join {{ ref('dim_region') }} r on e.region = r.region
group by 1, 2, 3
""")

write(MARTS / "agg_hopitaux_par_region.sql", """select
    r.region_code,
    h.region,
    count(*) as nb_hopitaux
from {{ ref('stg_sante_hopitaux_2025') }} h
left join {{ ref('dim_region') }} r on h.region = r.region
group by 1, 2
""")

write(MARTS / "schema.yml", """version: 2

models:
  - name: agg_etablissements_par_region_2023
    columns:
      - name: region
        data_tests: [not_null]
      - name: nb_etablissements
        data_tests: [not_null]
  - name: agg_hopitaux_par_region
    columns:
      - name: region
        data_tests: [not_null]
      - name: region_code
        data_tests:
          - not_null:
              config:
                severity: warn
""")