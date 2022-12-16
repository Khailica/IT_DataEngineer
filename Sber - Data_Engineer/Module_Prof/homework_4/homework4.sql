/*
1. На основе таблицы de11tm.HW4_FCT_TRANSACTION (хранятся транзакции, совершенные в 2021 году)
для каждого клиента в разрезе квартала транзакции построить следующие агрегаты:

        Сумма последней транзакции по клиенту за квартал;
        Кол-во транзакций по клиенту за квартал;
        Сумма максимальной транзакции по клиенту за квартал;
        Сумма средней транзакции по клиенту за квартал.
*/


SELECT
     *
FROM
     de11tm.hw4_fct_transaction
WHERE
     client_id = 37646 AND
     trans_dt = '2021-06-30'
ORDER BY
     client_id
,    trans_dt DESC;


SELECT
     client_id
,    date_part('quarter', hft.trans_dt) quarter
,    max(hft.trans_dt)
FROM
     de11tm.hw4_fct_transaction hft
WHERE
     hft.client_id = 37646
GROUP BY
     hft.client_id
,    date_part('quarter', hft.trans_dt)
ORDER BY
     hft.client_id
,    date_part('quarter', hft.trans_dt)



--EXPLAIN 
SELECT DISTINCT
     client_id
,    date_part('quarter', hft.trans_dt) AS quarter
,    last_value(hft.trans_amt) OVER(PARTITION BY hft.client_id, date_part('quarter', hft.trans_dt)
          ORDER BY hft.client_id, date_part('quarter', hft.trans_dt)) AS sum_last_day_trans
,    sum(hft.trans_amt) OVER w AS trans_amt
,    max(hft.trans_amt) OVER w AS trans_max
,    round(avg(hft.trans_amt) OVER w, 0) AS trans_avg
FROM
     de11tm.hw4_fct_transaction hft
WINDOW
     w AS(PARTITION BY hft.client_id, date_part('quarter', hft.trans_dt))
ORDER BY
     client_id
,    date_part('quarter', hft.trans_dt);

     
     

/*
2. На основе таблицы de11tm.HW4_FCT_TRANSACTION для каждого клиента в разрезе месяца вывести:
        Сумму всех транзакций в текущем месяце;
        Сумму всех транзакций в предыдущем месяце;
        Сумму всех транзакций в следующем месяце;
        Аддитивную сумму всех транзакций с начала года;
        Сумму транзакций в скользящем окне [-2 месяца; +1 месяц] относительно отчетного месяца.
*/
  	 

SELECT
     client_id
,    trans_dt
,    trans_amt
FROM
     de11tm.hw4_fct_transaction hft; 
 	 
  	  

/*
3* На основе таблицы de11tm.HW4_FCT_TRANSACTION для каждого клиента
в каждом месяце вывести предпоследнюю транзакцию
(Если по клиенту несколько транзакций прошло в один день, то выведите их все).
*/
 	

SELECT
     client_id
,    trans_dt
,    trans_amt
FROM
     de11tm.hw4_fct_transaction hft; 


 


	 