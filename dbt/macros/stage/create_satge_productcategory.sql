{% macro create_stage_productcategory() %}

    {% set sql %}  

        create TABLE if not exists stage.productcategory(
            productcategoryid integer,
            name varchar(50),
            modifieddate timestamp,
            _load_dttm timestamp,
            _batch_id varchar(36),
            _source_system varchar(50)
        )
        DISTRIBUTED BY (productcategoryid);
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}