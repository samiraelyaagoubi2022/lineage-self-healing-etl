-- Chaque ligne du staging doit porter l'empreinte (sha256) enregistrée dans raw._provenance pour sa table.
-- Détecte un fichier remplacé entre deux chargements, ou un staging qui mélange deux versions d'un fichier.
-- Le test renvoie une ligne par table en désaccord.
with obtenu as (
    select 'education_primaire_public_2023' as table_brute, sha256_source from {{ ref('stg_education_primaire_public_2023') }}
    union all select 'education_primaire_public_2026', sha256_source from {{ ref('stg_education_primaire_public_2026') }}
    union all select 'education_college_public_2023', sha256_source from {{ ref('stg_education_college_public_2023') }}
    union all select 'education_college_public_2026', sha256_source from {{ ref('stg_education_college_public_2026') }}
    union all select 'education_qualifiant_public_2023', sha256_source from {{ ref('stg_education_qualifiant_public_2023') }}
    union all select 'education_qualifiant_public_2026', sha256_source from {{ ref('stg_education_qualifiant_public_2026') }}
    union all select 'education_prive_2023', sha256_source from {{ ref('stg_education_prive_2023') }}
    union all select 'education_prive_2026', sha256_source from {{ ref('stg_education_prive_2026') }}
    union all select 'sante_hopitaux_2025', sha256_source from {{ ref('stg_sante_hopitaux_2025') }}
),

par_table as (
    select table_brute, count(distinct sha256_source) as nb_empreintes, min(sha256_source) as empreinte
    from obtenu
    group by table_brute
)

select p.table_brute, p.nb_empreintes, p.empreinte as empreinte_staging, v.sha256 as empreinte_provenance
from par_table p
join {{ source('raw', '_provenance') }} v using (table_brute)
where p.nb_empreintes <> 1 or p.empreinte <> v.sha256
