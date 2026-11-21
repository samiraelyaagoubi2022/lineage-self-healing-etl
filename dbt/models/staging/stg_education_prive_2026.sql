select
    trim("NOM_ETABL") as etablissement,
    trim("LL_COM")   as commune,
    trim("LL_PROV")  as province,
    trim("LL_REG")   as region,
    'prive'      as niveau,
    _version          as version,
    _sha256           as sha256_source
from {{ source('raw', 'education_prive_2026') }}
