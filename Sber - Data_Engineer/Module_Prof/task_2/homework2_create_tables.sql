
-- Создаю таблицы согласно ER-диаграммы БД ‘Транзакции клиента’	


-- DROP TABLE de11tm.ykir_Currency_types;
CREATE TABLE IF NOT EXISTS de11tm.ykir_Currency_types (
	id serial NOT NULL,
	title varchar(16),
	CONSTRAINT ykir_currency_types_pkey PRIMARY KEY (id)
);


-- DROP TABLE de11tm.ykir_Locators;
CREATE TABLE IF NOT EXISTS de11tm.ykir_Locators (
	locator_id serial NOT NULL,
	phone_id varchar(20),
	email varchar(64),
	CONSTRAINT ykir_locators_pkey PRIMARY KEY (locator_id)
);


-- DROP TABLE de11tm.ykir_Transactions;
CREATE TABLE IF NOT EXISTS de11tm.ykir_Transactions (
	id serial NOT NULL,
	client_id integer,
	money_amount numeric(10, 2) NULL,
	currency_id integer,
	CONSTRAINT ykir_transactions_pkey PRIMARY KEY (id)
);


-- DROP TABLE de11tm.ykir_Currency_exchange ;
CREATE TABLE IF NOT EXISTS de11tm.ykir_Currency_exchange (
	id serial NOT NULL,
	to_currency_id integer,
	coeff_number numeric(5, 3) NULL
);



-- DROP TABLE de11tm.ykir_Client;
CREATE TABLE IF NOT EXISTS de11tm.ykir_Client (
	id serial NOT NULL,
	"name" varchar(64),
	lastname varchar(64),
	locator_id integer,
	city varchar(64),
	CONSTRAINT ykir_client_pkey PRIMARY KEY (id)
);




ALTER TABLE de11tm.ykir_Transactions ADD CONSTRAINT ykir_Transactions_fk0 FOREIGN KEY (currency_id) REFERENCES de11tm.ykir_Currency_types(id);
ALTER TABLE de11tm.ykir_Transactions ADD CONSTRAINT ykir_Transactions_fk1 FOREIGN KEY (client_id) REFERENCES de11tm.ykir_Client(id);
ALTER TABLE de11tm.ykir_Currency_exchange ADD CONSTRAINT ykir_Currency_exchange_fk0 FOREIGN KEY (to_currency_id) REFERENCES de11tm.ykir_Currency_types(id);
ALTER TABLE de11tm.ykir_Currency_exchange CONSTRAINT ykir_currency_exchange_fk0 FOREIGN KEY (id) REFERENCES de11tm.ykir_currency_types(id),
ALTER TABLE de11tm.ykir_Currency_exchange CONSTRAINT ykir_currency_exchange_fk1 FOREIGN KEY (to_currency_id) REFERENCES de11tm.ykir_currency_types(id)
ALTER TABLE de11tm.ykir_Client ADD CONSTRAINT ykir_Client_fk0 FOREIGN KEY (locator_id) REFERENCES de11tm.ykir_Locators(locator_id);


-- заполняю данными (вставляем из консоли от имени клиента)
\COPY de11tm.ykir_Currency_types(id, title) FROM '/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_2/ykir_Currency_types.csv' WITH DELIMITER ',' CSV HEADER;

\COPY de11tm.ykir_Locators(locator_id, phone_id, email) FROM '/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_2/ykir_Locators.csv' WITH DELIMITER ',' CSV HEADER;

\COPY de11tm.ykir_Client(id, "name", lastname, locator_id, city) FROM '/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_2/ykir_Client.csv' WITH DELIMITER ',' CSV HEADER;

\COPY de11tm.ykir_Transactions(id, client_id, money_amount, currency_id) FROM '/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_2/ykir_Transactions.csv' WITH DELIMITER ',' CSV HEADER;

\COPY de11tm.ykir_Currency_exchange(id, to_currency_id, coeff_number) FROM '/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_2/ykir_Currency_exchange.csv' WITH DELIMITER ',' CSV HEADER;

	

--После всех изменений удаляем созданные объекты
 	  
TRUNCATE
	de11tm.ykir_currency_types
,	de11tm.ykir_currency_exchange
,	de11tm.ykir_transactions
,	de11tm.ykir_locators
,	de11tm.ykir_client
	CASCADE;	  


DROP TABLE IF EXISTS 
	de11tm.ykir_currency_types
,	de11tm.ykir_currency_exchange
,	de11tm.ykir_transactions
,	de11tm.ykir_locators
,	de11tm.ykir_client
	CASCADE;	
		
			
DROP VIEW IF EXISTS
	de11tm.flag_contact
,	de11tm.amount_trans;





  
 

