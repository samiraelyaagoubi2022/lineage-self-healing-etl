with etab as (
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
