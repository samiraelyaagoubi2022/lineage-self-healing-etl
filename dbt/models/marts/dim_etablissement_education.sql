-- Tous les établissements d'éducation (8 modèles de staging), une ligne par établissement et par version de fichier.
-- La région est normalisée ICI (macro normalize_region), pas dans le staging : le staging garde le libellé de la source,
-- ce qui permet aux tests existants de continuer à détecter la dérive 2023 / 2026.
{% set modeles = [
    'stg_education_primaire_public_2023',
    'stg_education_primaire_public_2026',
    'stg_education_college_public_2023',
    'stg_education_college_public_2026',
    'stg_education_qualifiant_public_2023',
    'stg_education_qualifiant_public_2026',
    'stg_education_prive_2023',
    'stg_education_prive_2026'
] %}

with etab as (
{% for m in modeles %}
    select * from {{ ref(m) }}
    {% if not loop.last %}union all by name{% endif %}
{% endfor %}
)

select
    e.etablissement,
    e.commune,
    e.province,
    e.region as region_source,
    {{ normalize_region('e.region') }} as region,
    r.region_code,
    e.niveau,
    e.version as version_source,
    e.sha256_source
from etab e
left join {{ ref('dim_region') }} r
    on {{ normalize_region('e.region') }} = r.region
