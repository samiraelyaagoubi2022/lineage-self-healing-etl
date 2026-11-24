select *
from {{ ref('stg_education_primaire_public_2023') }}
where etablissement is null