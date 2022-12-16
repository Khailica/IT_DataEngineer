/*
1. На основе таблицы de11tm.HW4_FCT_TRANSACTION (хранятся транзакции, совершенные в 2021 году)
для каждого клиента в разрезе квартала транзакции построить следующие агрегаты:

        Сумма последней транзакции по клиенту за квартал;
        Кол-во транзакций по клиенту за квартал;
        Сумма максимальной транзакции по клиенту за квартал;
        Сумма средней транзакции по клиенту за квартал.
*/


SELECT DISTINCT
     client_id
,    date_part('quarter', trans_dt) AS "quarter"
,    last_value(trans_amt) OVER(PARTITION BY client_id, date_part('quarter', trans_dt) ORDER BY client_id, date_part('quarter', trans_dt)) AS sum_last_day_trans
,    sum(trans_amt) OVER w AS trans_amt
,    max(trans_amt) OVER w AS trans_max
,    round(avg(trans_amt) OVER w, 0) AS trans_avg
FROM
     de11tm.hw4_fct_transaction
WINDOW
     w AS(PARTITION BY client_id, date_part('quarter', trans_dt))
ORDER BY
     client_id
,    date_part('quarter', trans_dt);

     
     

/*
2. На основе таблицы de11tm.HW4_FCT_TRANSACTION для каждого клиента в разрезе месяца вывести:
        Сумму всех транзакций в текущем месяце;
        Сумму всех транзакций в предыдущем месяце;
        Сумму всех транзакций в следующем месяце;
        Аддитивную сумму всех транзакций с начала года;
        Сумму транзакций в скользящем окне [-2 месяца; +1 месяц] относительно отчетного месяца.
*/
  	 

WITH cte AS (
     SELECT
          client_id
     ,    date_part('month', trans_dt) AS current_month
     ,    sum(trans_amt) AS sum_trans
     FROM
          de11tm.hw4_fct_transaction
     GROUP BY
          client_id
     ,    date_part('month', trans_dt)
)
SELECT
     *
,    lag(sum_trans) OVER(PARTITION BY client_id ORDER BY current_month) AS sum_trans_1preceding_month
,    lead(sum_trans) OVER(PARTITION BY client_id ORDER BY current_month) AS sum_trans_1following_month
,    sum(sum_trans) OVER(PARTITION BY client_id ORDER BY current_month) AS sum_trans_additive
,    sum(sum_trans) OVER(PARTITION BY client_id ORDER BY current_month ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS sum_trans_sliding_window
FROM
     cte
ORDER BY
     client_id
,    current_month;
 	 
  	  


/*
3* На основе таблицы de11tm.HW4_FCT_TRANSACTION для каждого клиента
в каждом месяце вывести предпоследнюю транзакцию
(Если по клиенту несколько транзакций прошло в один день, то выведите их все).
*/
 	
WITH cte1 AS (
     SELECT
          client_id
     ,    date_part('month', trans_dt) AS "month"
     ,    trans_dt
     ,    dense_rank() OVER(PARTITION BY client_id, date_part('month', trans_dt) ORDER BY trans_dt) AS grade
     ,    trans_amt
     FROM
          de11tm.hw4_fct_transaction
     ORDER BY
          client_id
     ,    date_part('month', trans_dt)
)
, cte2 AS (
     SELECT
          client_id
     ,    month
     ,    max(grade) - 1 AS penultimate_grade
     FROM
          cte1
     GROUP BY
          client_id
     ,    month
     ORDER BY
          client_id
     ,    month
)
SELECT
     cte1.client_id
,    cte1."month"
,    cte1.trans_dt
,    cte1.trans_amt
FROM
     cte2
JOIN
     cte1
          ON cte1.client_id = cte2.client_id AND
          cte1."month" = cte2."month" AND
          cte1.grade = cte2.penultimate_grade;
	 