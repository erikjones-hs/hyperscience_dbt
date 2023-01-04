{# For BigQuery need to add backticks #}
{% macro postgres__rename_relation(from_relation, to_relation) -%}
  {% do drop_relation(to_relation) %}
  {% call statement('rename_relation') -%}
    alter {{ to_relation.type }} {{ from_relation.database }}.{{ from_relation.schema }}.{{ from_relation.identifier }}
    rename to {{ to_relation.database }}.{{ to_relation.schema }}.{{ to_relation.identifier }}
  {%- endcall %}
{% endmacro %}

{# For BigQuery need to add backticks #}
{% macro postgres__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation.database }}.{{ relation.schema }}.{{ relation.identifier }}
  {%- endcall %}
{% endmacro %}

{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create or replace table {{ relation }}
  as (
    {{ sql }}
  );
{%- endmacro %}
