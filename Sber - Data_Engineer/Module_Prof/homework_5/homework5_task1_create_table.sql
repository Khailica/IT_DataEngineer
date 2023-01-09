/*
1.Для базы данных, которую вы создали после первого вебинара, напишите DDL код для создания таблиц. 
Проверяться будут соответствие имен, типизация, ограничения целостности. 
Все, что изображено на ER-диаграмме должно быть отражено в коде.
В ответе прикрепите ER-диаграмму из первого задания и .sql файл с кодом DDL
*/



--https://dbdesigner.page.link/o9vPt5GN3nXZEUrM7


--DROP TABLE de11tm.ykir_car
CREATE TABLE IF NOT EXISTS de11tm.ykir_car(
     car_id
          serial4
               NOT NULL
,    model_id
          integer
               NOT NULL
,    production_date
          date
,    CONSTRAINT car_pk 
          PRIMARY KEY(car_id)
);



--DROP TABLE de11tm.ykir_car_model
CREATE TABLE IF NOT EXISTS de11tm.ykir_car_model(
     model_id
          serial4
               NOT NULL
,    specifications
          text
,    CONSTRAINT car_model_pk 
          PRIMARY KEY(model_id)
);



--DROP TABLE de11tm.ykir_client
CREATE TABLE IF NOT EXISTS de11tm.ykir_client(
     client_id
          serial4
               NOT NULL
,    first_name
          varchar(20)
,    last_name
          varchar(20)
               NOT NULL
,    phone
          varchar(20)
,    CONSTRAINT client_pk 
          PRIMARY KEY(client_id)
);



--DROP TABLE de11tm.ykir_sales;
CREATE TABLE IF NOT EXISTS de11tm.ykir_sales(
     sale_id
          serial4
               NOT NULL
,    car_id
          integer
               NOT NULL
,    client_id
          integer
               NOT NULL
,    manager_id
          integer
               NOT NULL
,    sale_date
          date
,    total_price
          numeric(10, 2)
,    quantity
          integer
,    CONSTRAINT sales_pk 
          PRIMARY KEY(sale_id)
);



CREATE TABLE IF NOT EXISTS de11tm.ykir_manager(
     manager_id
          serial4
               NOT NULL
,    first_name
          varchar(20)
,    last_name
          varchar(20)
               NOT NULL
,    phone
          varchar(20)
,    CONSTRAINT manager_pk 
          PRIMARY KEY(manager_id)
);



ALTER TABLE de11tm.ykir_car
     ADD CONSTRAINT car_fk0 FOREIGN KEY(model_id)
          REFERENCES de11tm.ykir_car_model(model_id);
     
ALTER TABLE de11tm.ykir_sales
     ADD CONSTRAINT sales_fk0 FOREIGN KEY(car_id)
          REFERENCES de11tm.ykir_car(car_id);
     
ALTER TABLE de11tm.ykir_sales
     ADD CONSTRAINT sales_fk1 FOREIGN KEY(client_id)
          REFERENCES de11tm.ykir_client(client_id);
     
ALTER TABLE de11tm.ykir_sales
     ADD CONSTRAINT sales_fk2 FOREIGN KEY(manager_id)
          REFERENCES de11tm.ykir_manager(manager_id);
     
     
TRUNCATE TABLE
     de11tm.ykir_car
,    de11tm.ykir_car_model
,    de11tm.ykir_client
,    de11tm.ykir_sales
,    de11tm.ykir_manager
     CASCADE;
				
DROP TABLE IF EXISTS
     de11tm.ykir_car
,    de11tm.ykir_car_model
,    de11tm.ykir_client
,    de11tm.ykir_sales
,    de11tm.ykir_manager
     CASCADE;