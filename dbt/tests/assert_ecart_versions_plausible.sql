{{ config(severity='warn') }}
-- Avertit si le nombre d'établissements d'une région et d'un niveau change de plus de 30 % entre 2023 et 2026,
-- ou si une combinaison existe dans une seule des deux versions. Le seuil est à ajuster avec var('seuil_ecart_pct').
select region, niveau, nb_2023, nb_2026, ecart_pct
from {{ ref('comparaison_versions_par_region') }}
where (nb_2023 = 0) <> (nb_2026 = 0)
   or abs(ecart_pct) > {{ var('seuil_ecart_pct', 30) }}
