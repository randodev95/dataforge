{% macro test_unique(model, column) %}
SELECT {{ column }}
FROM {{ model }}
GROUP BY {{ column }}
HAVING COUNT(*) > 1
{% endmacro %}

{% macro test_not_null(model, column) %}
SELECT *
FROM {{ model }}
WHERE {{ column }} IS NULL
{% endmacro %}

{% macro test_accepted_values(model, column, values) %}
SELECT {{ column }}
FROM {{ model }}
WHERE {{ column }} NOT IN (
    {% for val in values %}
    '{{ val }}'{{ "," if not loop.last }}
    {% endfor %}
)
{% endmacro %}
