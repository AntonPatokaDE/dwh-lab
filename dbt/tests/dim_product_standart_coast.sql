--Может ли standart_coast быть отрицательной
SELECT productid
FROM {{ref('dim_product')}}
WHERE standardcost < 0