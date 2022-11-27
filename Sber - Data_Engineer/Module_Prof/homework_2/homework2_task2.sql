/*
1) Для каждого клиента посчитать кол-во и сумму транзакций в рублях (для перевода
из других валют в рубли следует использовать таблицу
HW2_CURRENCY_EXCHANGE посредством join)
*/

SELECT c.id,
	   count(t.id),
	   sum(t.money_amount * COALESCE(ce.coeff_number, 1)) AS amount_rub
  FROM ykir_transactions t 
       LEFT JOIN ykir_currency_exchange ce ON ce.id  = t.currency_id
      		AND ce.to_currency_id = 1
       JOIN ykir_client c ON c.id = t.client_id 
 GROUP BY c.id
 ORDER BY c.id;
 



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
 	  
 SELECT c.id, 
 		CASE WHEN l.email IS NULL AND  l.phone_id  IS NULL THEN 'N'
 			  ELSE 'Y'
 		END AS flag
  FROM ykir_client c
  	   LEFT JOIN ykir_locators l ON l.locator_id = c.locator_id;
  	  
  	  
  	  
  	  
/*
4) Для каждого клиента рассчитать флаг ‘Кол-во транзакций более 5’: если было
совершено более 5 транзакций, то флаг принимает значение ‘Y’, иначе ‘N’
*/
  	  
WITH amount_trans AS ( 
	 SELECT t.client_id ,
	 		count(t.id) AS amount_trans
	   FROM ykir_transactions t
	  GROUP BY t.client_id 
	  )

SELECT DISTINCT(t.client_id),
	   CASE WHEN a.amount_trans > 5 THEN 'Y'
	   		ELSE 'N'
	   END AS flag
  FROM ykir_transactions t
 	   JOIN amount_trans a ON a.client_id = t.client_id
 ORDER BY t.client_id;




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
WITH amount_trans AS ( 
	 SELECT t.client_id ,
	 		count(t.id) AS amount_trans
	   FROM ykir_transactions t
	  GROUP BY t.client_id
	  ),
	  
	  flag_contact AS (
	  SELECT c.id, 
 	  		 CASE WHEN l.email IS NULL AND  l.phone_id  IS NULL THEN 'N'
 	  	 		  ELSE 'Y'
 			  END AS flag
  	   FROM ykir_client c
  	   		LEFT JOIN ykir_locators l ON l.locator_id = c.locator_id
  	   ),
  	   
  	   sum_trans AS (
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
  	   JOIN amount_trans a ON a.client_id = t.client_id 
  	   JOIN flag_contact fc ON fc.id = t.client_id
  	   JOIN sum_trans st ON st.id = t.client_id 
 WHERE fc.flag = 'Y'
 	   AND a.amount_trans > 5
 ORDER BY t.client_id;
 
 
 /*
 Comment:
 
в 1 и 4 запросах не учтены клиенты, у которых не было транзакций.
В 4 запросе можно было не использовать CTE, а ограничиться соединением таблиц с транзакциями и клиентами.
Для 5-го запроса лучше было использовать 3 и 4 запрос в виде view.
Соответственно, из 3 запроса забираем только клиентов с флагом 'Y', а в 4 запросе уже содержатся клиенты с более 5 транзакциями,
даже не нужно фильтроваться
 */

	 