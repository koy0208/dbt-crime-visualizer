{% macro kanji_to_num(column_name) %}
    replace(translate({{ column_name }}, '一二三四五六七八九', '123456789'), '十', '10')
{% endmacro %}