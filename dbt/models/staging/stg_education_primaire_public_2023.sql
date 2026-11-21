select
    trim("NOM_ETABL") as etablissement,
    trim("COMMUNE")   as commune,
    trim("PROVINCE")  as province,
    trim("REGION")   as region,
    'primaire_public'      as niveau,
    _version          as version,
    _sha256           as sha256_source
from {{ source('raw', 'education_primaire_public_2023') }}
