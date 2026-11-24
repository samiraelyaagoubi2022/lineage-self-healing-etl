select
    r.region_code,
    h.region,
    count(*) as nb_hopitaux
from {{ ref('stg_sante_hopitaux_2025') }} h
left join {{ ref('dim_region') }} r on h.region = r.region
group by 1, 2
