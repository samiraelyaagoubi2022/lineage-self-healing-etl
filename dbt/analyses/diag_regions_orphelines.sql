with dim as (select region from {{ ref('dim_region') }})
select '2026_college' as source, region, count(*) as nb
from {{ ref('stg_education_college_public_2026') }}
where region not in (select region from dim) group by 1, 2
union all
select '2026_primaire', region, count(*)
from {{ ref('stg_education_primaire_public_2026') }}
where region not in (select region from dim) group by 1, 2
union all
select '2026_prive', region, count(*)
from {{ ref('stg_education_prive_2026') }}
where region not in (select region from dim) group by 1, 2
union all
select '2026_qualifiant', region, count(*)
from {{ ref('stg_education_qualifiant_public_2026') }}
where region not in (select region from dim) group by 1, 2
union all
select '2025_hopitaux', region, count(*)
from {{ ref('stg_sante_hopitaux_2025') }}
where region not in (select region from dim) group by 1, 2
order by 1, 3 desc