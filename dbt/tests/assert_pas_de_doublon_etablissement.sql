{{ config(severity='warn') }}
-- Avertit si un même établissement apparaît plusieurs fois dans un même fichier
-- (même version, même niveau, même nom, même commune). Les lignes sans nom ou sans commune sont ignorées.
select version_source, niveau, etablissement, commune, count(*) as nb
from {{ ref('dim_etablissement_education') }}
where etablissement is not null and commune is not null
group by version_source, niveau, etablissement, commune
having count(*) > 1
