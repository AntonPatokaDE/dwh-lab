{% macro create_stage_productsubcategory() %}

    {% set sql %}  

        create TABLE if not exists stage.productsubcategory(
            productsubcategoryid integer,
            productcategoryid integer,
            name varchar(50),
            modifieddate timestamp,
            _load_dttm timestamp,
            _batch_id varchar(36),
            _source_system varchar(50)
        )
        DISTRIBUTED BY (productsubcategoryid);
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}