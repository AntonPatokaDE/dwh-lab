{% macro create_stage_schema() %}

    {% set sql %}  

        create schema if not exists stage;
        
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}