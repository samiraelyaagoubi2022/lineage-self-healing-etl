-- Le staging ne doit ni perdre ni dupliquer de lignes par rapport au brut.
-- Le nombre attendu vient de raw._provenance (rempli par l'ingestion).
-- Le test renvoie une ligne par table en désaccord : s'il renvoie des lignes, il échoue.
with attendu as (
    select table_brute, nb_lignes
    from {{ source('raw', '_provenance') }}
),

obtenu as (
    select 'education_primaire_public_2023' as table_brute, count(*) as nb from {{ ref('stg_education_primaire_public_2023') }}
    union all select 'education_primaire_public_2026', count(*) from {{ ref('stg_education_primaire_public_2026') }}
    union all select 'education_college_public_2023', count(*) from {{ ref('stg_education_college_public_2023') }}
    union all select 'education_college_public_2026', count(*) from {{ ref('stg_education_college_public_2026') }}
    union all select 'education_qualifiant_public_2023', count(*) from {{ ref('stg_education_qualifiant_public_2023') }}
    union all select 'education_qualifiant_public_2026', count(*) from {{ ref('stg_education_qualifiant_public_2026') }}
    union all select 'education_prive_2023', count(*) from {{ ref('stg_education_prive_2023') }}
    union all select 'education_prive_2026', count(*) from {{ ref('stg_education_prive_2026') }}
    union all select 'sante_hopitaux_2025', count(*) from {{ ref('stg_sante_hopitaux_2025') }}
)

select a.table_brute, a.nb_lignes as attendu, o.nb as obtenu
from attendu a
join obtenu o using (table_brute)
where a.nb_lignes <> o.nb
