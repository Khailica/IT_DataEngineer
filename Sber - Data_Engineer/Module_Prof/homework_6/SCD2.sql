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



-- вставляем из консоли от имени клиента
\COPY de11tm.ykir_incremental(id, val, update_dt) FROM '/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_6/ykir_incremental.csv' WITH DELIMITER ',' CSV HEADER;



--0.начальное решение (самая первая загрузка всего источника в приемник)

--создаем приемник
DROP TABLE IF EXISTS de11tm.ykir_tfes_target;
CREATE TABLE de11tm.ykir_tfes_target(
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
CREATE TABLE de11tm.ykir_tfes_stg(
     id
          integer
,    val
          varchar(10)
,    update_dt
          timestamp
);


--создаем таблица стейджинга для удаления записей
DROP TABLE IF EXISTS de11tm.ykir_tfes_stg_delete;
CREATE TABLE de11tm.ykir_tfes_stg_delete(
     id
          integer
);


--создаем таблицу с мета-данными
DROP TABLE IF EXISTS de11tm.ykir_tfes_metadata;
CREATE TABLE de11tm.ykir_tfes_metadata(
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



--добавление в таблицу с метаданными информации о последней заливке данных
INSERT INTO de11tm.ykir_tfes_metadata
SELECT
     max(effective_from_dttm)
FROM
     de11tm.ykir_tfes_target;

COMMIT;



-- заливка новой сточки в таблицу-источник
INSERT INTO de11tm.ykir_incremental(
     id
,    val
,    update_dt
)
VALUES(
     4
,    'D'
,    '2022-12-30 00:00:00'
);

COMMIT;



--скрипт захвата инкремента из таблицы источника и заливка в таблицу-приемник

--1.Подготовка стейджинга
DELETE FROM
     de11tm.ykir_tfes_stg;
DELETE FROM
     de11tm.ykir_tfes_stg_delete;


--забираем из таблицы приемника новые строки для обработки update и delete
INSERT INTO de11tm.ykir_tfes_stg
SELECT
     id
,    val
,    update_dt
FROM
     de11tm.ykir_incremental
WHERE
     update_dt > (
          SELECT
               max_update_dt
          FROM
               de11tm.ykir_tfes_metadata
     );


--заливаем все идентификаторы из таблицы ykir_incremental для удаления
INSERT INTO de11tm.ykir_tfes_stg_delete
SELECT
     id
FROM
     de11tm.ykir_incremental;




--2.Обработка insert, update и delete в приемнике
--вставляем из стейджинговой таблицы в таблицу-приемник новые записи 
--(обработка insert)					   
INSERT INTO de11tm.ykir_tfes_target(
     id
,    val
,    effective_from_dttm
)
SELECT
     stg.id
,    stg.val
,    stg.update_dt
FROM
     de11tm.ykir_tfes_stg AS stg
LEFT JOIN
     de11tm.ykir_tfes_target AS tgt
          ON stg.id = tgt.id;

--COMMIT;


          
--обновляем строки в таблице-приемнике по старым записям
--(обработка update)
UPDATE
     de11tm.ykir_tfes_target AS targ
SET
     effective_to_dttm = tmp.effective_to_dttm
FROM
     (
          SELECT
               tgt.id
          ,    tgt.val
          ,    tgt.effective_from_dttm
          ,    coalesce(
                    lead(tgt.effective_from_dttm) OVER(PARTITION BY tgt.id ORDER BY tgt.effective_from_dttm) - '1 second'::interval
               ,    '5999-12-31 00:00:00'
               ) AS effective_to_dttm
          FROM
               de11tm.ykir_tfes_target AS tgt
          JOIN
               de11tm.ykir_tfes_stg AS stg
                    ON stg.id = tgt.id
     ) tmp
JOIN
     de11tm.ykir_tfes_stg AS stg
          ON stg.id = tmp.id AND
          tmp.effective_from_dttm < stg.update_dt
WHERE
     targ.id = tmp.id AND
     targ.effective_from_dttm = tmp.effective_from_dttm;



TABLE de11tm.ykir_incremental;
TABLE de11tm.ykir_tfes_metadata;
TABLE de11tm.ykir_tfes_stg;
TABLE de11tm.ykir_tfes_stg_delete;
TABLE de11tm.ykir_tfes_target;



--удаляем из таблицы-приемника записей, удаленных на источнике
--(обработка delete)
delete from de11tm.ykir_tfes_target
where id in (
Select 
	tgt.id
from de11tm.ykir_tfes_target tgt 
	left join de11tm.ykir_tfes_stg_delete stg
	on tgt.id = stg.id
where stg.id is null)


--обновим метаданные
update ykir_ykir_tfes_metadata set max_update_date = 
		(Select max(UPDATE_DT) from ykir_tfes_stg);


--и вот только теперь имеем право выполнить коммит
commit;
