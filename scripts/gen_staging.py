import pathlib

DBT = pathlib.Path("dbt")
STG = DBT / "models" / "staging"
MAC = DBT / "macros"
STG.mkdir(parents=True, exist_ok=True)
MAC.mkdir(parents=True, exist_ok=True)


def write(path, text):
    path.write_text(text, encoding="utf-8")
    print("écrit", path)


# 1) Configuration du projet : schémas staging et marts
write(DBT / "dbt_project.yml", """name: lineage_etl
version: "1.0.0"
config-version: 2
profile: lineage_etl

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]
target-path: target
clean-targets: ["target", "dbt_packages"]

models:
  lineage_etl:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
""")

# 2) Macro : utiliser exactement le nom de schéma demandé (staging, marts)
write(MAC / "generate_schema_name.sql", """{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
""")

# 3) Sources : les 9 tables brutes + la table de provenance
NIVEAUX = ("primaire_public", "college_public", "qualifiant_public", "prive")
ANNEES = {"2023": ("COMMUNE", "PROVINCE", "REGION"),
          "2026": ("LL_COM", "LL_PROV", "LL_REG")}
tables = [f"education_{n}_{a}" for n in NIVEAUX for a in ANNEES] + ["sante_hopitaux_2025"]

src = "version: 2\n\nsources:\n  - name: raw\n    schema: raw\n    tables:\n"
for t in tables + ["_provenance"]:
    src += f"      - name: {t}\n"
write(STG / "sources.yml", src)

# 4) Staging éducation : 8 modèles (2023 lit REGION, 2026 lit LL_REG)
TEMPLATE = """select
    trim("NOM_ETABL") as etablissement,
    trim("__COM__")   as commune,
    trim("__PROV__")  as province,
    trim("__REG__")   as region,
    '__NIVEAU__'      as niveau,
    _version          as version,
    _sha256           as sha256_source
from {{ source('raw', '__TABLE__') }}
"""
for niveau in NIVEAUX:
    for annee, (com, prov, reg) in ANNEES.items():
        table = f"education_{niveau}_{annee}"
        sql = (TEMPLATE.replace("__TABLE__", table).replace("__NIVEAU__", niveau)
               .replace("__COM__", com).replace("__PROV__", prov).replace("__REG__", reg))
        write(STG / f"stg_{table}.sql", sql)

# 5) Staging santé : hôpitaux 2025
write(STG / "stg_sante_hopitaux_2025.sql", """select
    trim("Etablissement hospitalier") as etablissement,
    trim("Catégorie")                 as categorie,
    trim("Commune")                   as commune,
    trim("Province/Délégation")       as province,
    trim("Région")                    as region,
    _version                          as version,
    _sha256                           as sha256_source
from {{ source('raw', 'sante_hopitaux_2025') }}
""")