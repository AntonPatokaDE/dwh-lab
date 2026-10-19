{% macro create_get_sales_function() %}
    {% set sql %}
    CREATE OR REPLACE FUNCTION dm_dbt.get_sales(
        date_group integer,
        category_id integer[],
        min_units integer
    )
    RETURNS TABLE (
        period timestamp,
        productcategoryid integer,
        category_name varchar,
        sales_amount numeric,
        units_sold bigint,
        distinct_orders bigint
    )
    AS $$
        SELECT 
            CASE
                WHEN date_group = 1 THEN date_trunc('day',d.date)
                WHEN date_group = 2 THEN date_trunc('week',d.date)
                WHEN date_group = 3 THEN date_trunc('month',d.date)
            END::timestamp AS period,
            p.productcategoryid,
            p.category_name,
            sum(f.line_amount) as sales_amount,
            sum(f.orderqty) as units_sold,
            count(distinct f.salesorderid) as distinct_orders
        FROM {{ ref('fact_sales') }} AS f
        INNER JOIN {{ ref('dim_date') }} AS d
            ON f.orderdate::date = d.date
        INNER JOIN {{ ref('dim_product') }} AS p
            ON f.productid = p.productid

        where p.productcategoryid = ANY(category_id)
        
        group by period,p.productcategoryid,p.category_name
        having sum(f.orderqty) > min_units
    $$
    LANGUAGE SQL;
    {% endset %}
    {% do run_query(sql) %}
{%- endmacro %}