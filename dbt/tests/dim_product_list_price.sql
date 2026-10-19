--Может ли listprice быть отрицательной
SELECT productid
FROM {{ref('dim_product')}}
WHERE listprice < 0