select
    trim("Etablissement hospitalier") as etablissement,
    trim("Catégorie")                 as categorie,
    trim("Commune")                   as commune,
    trim("Province/Délégation")       as province,
    trim("Région")                    as region,
    _version                          as version,
    _sha256                           as sha256_source
from {{ source('raw', 'sante_hopitaux_2025') }}
