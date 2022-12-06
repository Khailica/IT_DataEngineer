/*
1) Для каждого клиента посчитать кол-во и сумму транзакций в рублях (для перевода
из других валют в рубли следует использовать таблицу
HW2_CURRENCY_EXCHANGE посредством join)
*/

--EXPLAIN --(cost=102.50..102.92 rows=170 width=44)
SELECT c.id,
	   count(t.id) AS transaction_cnt,
	   COALESCE(sum(t.money_amount * COALESCE(ce.coeff_number, 1)), 0) AS amount_rub
  FROM de11tm.ykir_transactions t 
       LEFT JOIN de11tm.ykir_currency_exchange ce ON ce.id  = t.currency_id
      			 AND ce.to_currency_id = 1
       RIGHT JOIN de11tm.ykir_client c ON c.id = t.client_id 
 GROUP BY c.id
 ORDER BY c.id;


-- решение преподавателя:
--EXPLAIN --(cost=106.12..106.55 rows=170 width=44)
select
	hc.id,
	count(ht.client_id) as transaction_cnt,
	coalesce(sum(ht.money_amount*(
		case
			when ht.currency_id = 1 then 1 
			else hce.coeff_number 
		end)),0)
	as amount_rub
from de11tm.ykir_client hc
	left join de11tm.ykir_transactions ht
	on hc.id = ht.client_id
	left join de11tm.ykir_currency_exchange hce 
	on ht.currency_id = hce.id and hce.to_currency_id = 1
group by hc.id
order by hc.id;

/*
2) Для каждого клиента вывести его email и телефон. Если контакт не найден, то в
поле необходимо проставить ‘нет данных’
*/

SELECT c.id, COALESCE(l.email, 'Нет данных'), COALESCE(l.phone_id, 'Нет данных')
  FROM ykir_client c
  	   LEFT JOIN ykir_locators l ON l.locator_id = c.locator_id;
  	   

  	 
  	  
/*
3) Для каждого клиента необходимо рассчитать флаг наличия хотя бы одного
контакта: если хотя бы один контакт найден, то флаг принимает значение ‘Y’,
иначе ‘N’
*/
 	
 CREATE OR REPLACE VIEW de11tm.flag_contact AS ( 	  
 	SELECT c.id, 
 			CASE WHEN l.email IS NULL AND  l.phone_id  IS NULL THEN 'N'
 				  ELSE 'Y'
 			END AS flag
 	 FROM ykir_client c
 	 	   LEFT JOIN ykir_locators l ON l.locator_id = c.locator_id
 );
  	  
  	  
  	  
  	  
/*
4) Для каждого клиента рассчитать флаг ‘Кол-во транзакций более 5’: если было
совершено более 5 транзакций, то флаг принимает значение ‘Y’, иначе ‘N’
*/

CREATE OR REPLACE VIEW de11tm.amount_trans AS (
	SELECT DISTINCT(c.id),
		   count(t.id) AS amount_trans,
		   CASE WHEN count(t.id) > 5 THEN 'Y'
		   		ELSE 'N'
		   END AS flag
	  FROM ykir_client c
	 	   LEFT JOIN ykir_transactions t ON t.client_id = c.id 
	 GROUP BY c.id 
	 ORDER BY c.id
);



/*
5) Для выгрузки подготовить список клиентов, для которых указан хотя бы 1 контакт и совершивших более 5 транзакций.
В выгрузке должны содержаться следующие поля:

● Идентификатор клиента
● Фамилия клиента
● Имя клиента
● Телефон клиента
● e-mail клиента
● Сумма транзакций
*/

-- использую в подзапросах выборки из предыдущих заданий
WITH sum_trans AS (
  	 SELECT c.id,
	   		count(t.id),
	   		sum(t.money_amount * COALESCE(ce.coeff_number, 1)) AS amount_rub
	   FROM ykir_transactions t 
      		LEFT JOIN ykir_currency_exchange ce ON ce.id  = t.currency_id
      			 	  AND ce.to_currency_id = 1
       		JOIN ykir_client c ON c.id = t.client_id 
 	  GROUP BY c.id
)
  
SELECT DISTINCT(t.client_id) AS "Идентификатор клиента",
	   c.lastname AS "Фамилия",
	   c."name" AS "Имя",
	   l.phone_id AS "Телефон",
	   l.email "e-mail",
	   a.amount_trans AS "Кол-во транцакций",
	   st.amount_rub AS "Сумма транзакций в руб"
	   
  FROM ykir_transactions t
  	   JOIN ykir_client c ON c.id = t.client_id 
  	   JOIN ykir_locators l ON l.locator_id = c.locator_id
  	   JOIN amount_trans a ON a.id = t.client_id 
  	   JOIN flag_contact fc ON fc.id = t.client_id
  	   JOIN sum_trans st ON st.id = t.client_id 
 WHERE fc.flag = 'Y'
 	   AND a.flag = 'Y'
 ORDER BY t.client_id;
 



DROP VIEW IF EXISTS
			de11tm.flag_contact,
			de11tm.amount_trans; 
 
 /*
 Comment:
 
в 1 и 4 запросах не учтены клиенты, у которых не было транзакций.
В 4 запросе можно было не использовать CTE, а ограничиться соединением таблиц с транзакциями и клиентами.
Для 5-го запроса лучше было использовать 3 и 4 запрос в виде view.
Соответственно, из 3 запроса забираем только клиентов с флагом 'Y',
а в 4 запросе уже содержатся клиенты с более 5 транзакциями, даже не нужно фильтроваться.
 */

	 