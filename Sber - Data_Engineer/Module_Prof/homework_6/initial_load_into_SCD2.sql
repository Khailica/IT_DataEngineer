DROP TABLE IF EXISTS de11tm.ykir_incremental;
CREATE TABLE IF NOT EXISTS de11tm.ykir_incremental(
     id
          integer
               DEFAULT NULL
,    val
          varchar(10)
               DEFAULT NULL
,    update_dt
          timestamp
               DEFAULT NULL
);


-- заполняем таблицу данными (из консоли от имени клиента)
\COPY de11tm.ykir_incremental(id, val, update_dt) FROM '/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_6/ykir_incremental.csv' WITH DELIMITER ',' CSV HEADER;




--0.начальное решение (самая первая загрузка всего источника в приемник)

--создаем приемник
DROP TABLE IF EXISTS de11tm.ykir_tfes_target;
CREATE TABLE IF NOT EXISTS de11tm.ykir_tfes_target(
     id
          integer
,    val
          varchar(10)
,    effective_from_dttm
          timestamp
,    effective_to_dttm
          timestamp
               DEFAULT '5999-12-31 00:00:00'
,    deleted_flg
          smallint
               DEFAULT 0
);



--создаем стейджинговую таблицу
DROP TABLE IF EXISTS de11tm.ykir_tfes_stg;
CREATE TABLE IF NOT EXISTS de11tm.ykir_tfes_stg(
     id
          integer
,    val
          varchar(10)
,    update_dt
          timestamp
);



--создаем таблица стейджинга для удаления записей
DROP TABLE IF EXISTS de11tm.ykir_tfes_stg_delete;
CREATE TABLE IF NOT EXISTS de11tm.ykir_tfes_stg_delete(
     id
          integer
,    delete_dt
          timestamp
);



--создаем таблицу с мета-данными
DROP TABLE IF EXISTS de11tm.ykir_tfes_metadata;
CREATE TABLE IF NOT EXISTS de11tm.ykir_tfes_metadata(
     max_update_dt
          timestamp  --максимальная дата update_dt среди загруженных в таблицу-приемник
);


 
--загрузка начального решения
DELETE FROM
     ykir_tfes_target;

INSERT INTO de11tm.ykir_tfes_target
SELECT
     id
,    val
,    update_dt
,    COALESCE (
          LEAD(update_dt) OVER (PARTITION BY id ORDER BY update_dt) - '1 second'::INTERVAL
     ,    '5999-12-31 00:00:00'
     ) AS effective_to_dttm
FROM
     de11tm.ykir_incremental;



--добавление в таблицу с метаданными информации о последней заливке данных в таблицу-приемник SCD2
INSERT INTO de11tm.ykir_tfes_metadata
SELECT
     max(effective_from_dttm)
FROM
     de11tm.ykir_tfes_target;

COMMIT;



--просмотр таблиц
TABLE de11tm.ykir_incremental;
TABLE de11tm.ykir_tfes_metadata;
TABLE de11tm.ykir_tfes_stg;
TABLE de11tm.ykir_tfes_stg_delete;
TABLE de11tm.ykir_tfes_target;
SELECT * FROM de11tm.ykir_tfes_target ORDER BY id, effective_from_dttm;
