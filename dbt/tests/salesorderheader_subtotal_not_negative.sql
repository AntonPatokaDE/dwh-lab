SELECT *
FROM {{ ref('salesorderheader') }}
WHERE subtotal < 0