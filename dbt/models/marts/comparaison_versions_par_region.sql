-- Nombre d'établissements par région et par niveau dans le fichier 2023 et dans le fichier 2026.
with comptes as (
    select
        region,
        niveau,
        count(*) filter (where version_source = '2023') as nb_2023,
        count(*) filter (where version_source = '2026') as nb_2026
    from {{ ref('dim_etablissement_education') }}
    group by region, niveau
)

select
    *,
    nb_2026 - nb_2023 as ecart,
    round(100.0 * (nb_2026 - nb_2023) / nullif(nb_2023, 0), 1) as ecart_pct
from comptes
