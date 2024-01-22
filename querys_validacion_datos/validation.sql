--------- 1. QUERY ------------------

SELECT
    'my_user' AS nombre_tabla,
    COUNT(*) AS conteo_registros,
    AVG(price) AS valor_medio,
    MIN(price) AS valor_minimo,
    MAX(price) AS valor_maximo
FROM
    db_batch.public.my_user

UNION ALL

SELECT
    'my_user_validation' AS nombre_tabla,
    COUNT(*) AS conteo_registros,
    AVG(price) AS valor_medio,
    MIN(price) AS valor_minimo,
    MAX(price) AS valor_maximo
FROM
    db_batch.public.my_user_validation;

----------- 2. QUERY -----------------------

SELECT
    SUM(conteo_registros) AS total_registros,
    SUM(valor_medio) AS total_valor_medio,
    SUM(valor_minimo) AS total_valor_minimo,
    SUM(valor_maximo) AS total_valor_maximo
FROM (
         SELECT
             COUNT(*) AS conteo_registros,
             AVG(price) AS valor_medio,
             MIN(price) AS valor_minimo,
             MAX(price) AS valor_maximo
         FROM
             db_batch.public.my_user

         UNION ALL

         SELECT
             COUNT(*) AS conteo_registros,
             AVG(price) AS valor_medio,
             MIN(price) AS valor_minimo,
             MAX(price) AS valor_maximo
         FROM
             db_batch.public.my_user_validation
     ) AS total;

