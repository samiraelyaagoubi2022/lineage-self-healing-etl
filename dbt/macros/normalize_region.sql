{% macro normalize_region(col) %}
    case
        when trim({{ col }}) = 'Casablanca-Settat' then 'Grand Casablanca-Settat'
        else trim({{ col }})
    end
{% endmacro %}