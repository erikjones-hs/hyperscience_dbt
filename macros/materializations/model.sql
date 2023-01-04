{% macro model_options(ml_config, labels) %}
    {%- if labels -%}
        {%- set label_list = [] -%}
        {%- for label, value in labels.items() -%}
            {%- do label_list.append((label, value)) -%}
        {%- endfor -%}
        {%- do ml_config.update({'labels': label_list}) -%}
    {%- endif -%}

    {% set options -%}
        with (
            {%- for opt_key, opt_val in ml_config.items() -%}
                {%- if opt_val is sequence and not (opt_val | first) is number and (opt_val | first).startswith('hparam_') -%}
                    {{ opt_key }}={{ opt_val[0] }}({{ opt_val[1:] | join(', ') }})
                {%- else -%}
                    {{ opt_key }}={{ (opt_val | tojson) if opt_val is string else opt_val }}
                {%- endif -%}
                {{ ',' if not loop.last }}
            {%- endfor -%}
        )
    {%- endset %}

    {%- do return(options) -%}
{%- endmacro -%}

{% macro postgres__create_model_as(relation, sql) %}
    {%- set ml_config = config.get('ml_config', {}) -%}
    {%- set raw_labels = config.get('labels', {}) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    create or replace model {{ relation.identifier }}
    {{ model_options(
        ml_config=ml_config,
        labels=raw_labels
    ) }}
    as 
        {{ sql }};
{% endmacro %}

{% materialization model, adapter='postgres' -%}
    {%- set identifier = model['alias'] -%}
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}

    {% call statement('main') -%}
        {{ postgres__create_model_as(target_relation, sql) }}
    {% endcall -%}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
