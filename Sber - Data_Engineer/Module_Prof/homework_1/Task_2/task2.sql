
--Создать таблицу XXXX_CLIENT (в соответствии с ER-диаграммой), кудазагрузить клиентов
--и информацию о них с вкладки client
CREATE TABLE de11tm.ykir_Client (
	id integer PRIMARY KEY,
	"name" varchar(64),
	lastname varchar(64),
	locator_id integer,
	city varchar(64)
);


/*
-- вставляем из консоли от имени клиента
\COPY de11tm.ykir_Client(id, name, lastname, locator_id, city)
FROM
'/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_1/Task_2/client.csv'
WITH DELIMITER ',' CSV HEADER;
*/


--Переименовать столбец LASTNAME в LAST_NAME в таблице XXXX_CLIENT
ALTER TABLE de11tm.ykir_Client RENAME COLUMN lastname TO last_name;


--Изменить тип данных поля CITY на varchar(100) в таблице XXXX_CLIENT
ALTER TABLE de11tm.ykir_Client ALTER COLUMN city TYPE varchar(100);


--Создать представление XXXX_V_MOSCOW_CLIENT и записать туда всех клиентов из Москвы на основе
--созданной ранее таблицы CLIENT
CREATE VIEW de11tm.ykir_v_moscow_client AS
	SELECT *
	  FROM de11tm.ykir_client yc 
	 WHERE city LIKE 'Москва';
	
	
--Создать таблицу XXXX_CURRENCY_TYPES, куда загрузить данные с вкладки currency_types	
CREATE TABLE de11tm.ykir_currency_types (
	id integer PRIMARY KEY,
	title varchar(16)
);


-- вставляем из консоли от имени клиента
\COPY dde11tm.ykir_currency_types(id, title)
FROM
'/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_1/Task_2/currency_types.csv'
WITH DELIMITER ',' CSV HEADER;



CREATE TABLE de11tm.transactions (
	id int4 NULL,
	client_id int4 NULL,
	money_amount numeric(10, 2) NULL,
	currency_id int4 NULL
);


--Создать представление XXXX_V_TRANSACTIONS, в которое вывести все рублевые
--и долларовые транзакции на основе данных таблицы TRANSACTIONS (уже прогружена в нашу схему)
CREATE VIEW ykir_v_transactions AS
	SELECT *
	  FROM de11tm.transactions
	 WHERE currency_id IN (1, 2);
	

--Вывести все рублевые транзакции либо на очень маленькую сумму (меньше 10 рублей),
--либо на большую (больше 20 000 рублей)
SELECT *
  FROM ykir_v_transactions
 WHERE currency_id = 2
 	   AND (money_amount < 10 OR money_amount > 20000);

--После всех изменений удаляем созданные объекты + объекты, созданные Вами на занятии
DROP TABLE IF EXISTS ykir_car, ykir_car_model, ykir_client, ykir_currency_types, ykir_manager, ykir_sales CASCADE;
DROP VIEW IF EXISTS ykir_v_moscow_client, ykir_v_transactions CASCADE; 
	



  
 

