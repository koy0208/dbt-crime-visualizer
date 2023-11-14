{% macro replace_null(column_name) %}
    case when {{ column_name }} = '' then '不明' else {{ column_name }} end
{% endmacro %}