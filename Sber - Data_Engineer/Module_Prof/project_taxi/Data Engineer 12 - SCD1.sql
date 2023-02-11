TABLE de11tm.incremental;


--0.начальное решение (самая первая загрузка всего источника в приемник)

--создаем приемник
DROP TABLE ykir_target;
CREATE TABLE ykir_target(
     id
          integer
,    val
          varchar(10)
,    update_dt
          timestamp
);


--создаем стейджинговую таблицу
CREATE TABLE ykir_stg(
     id
          integer
,    val
          varchar(10)
,    update_dt
          timestamp
);


--создаем таблица стейджинга для удаления записей
CREATE TABLE ykir_stg_delete(
     id
          integer
);


--создаем таблицу с мета-данными
CREATE TABLE ykir_metadata(
     max_update_dt  -- максимальная дата update_dt среди загруженных в таблицу-приемник 
          timestamp
);


--загрузка начального решения
INSERT INTO ykir_target
SELECT
     id
,    val
,    update_dt
FROM
     de11tm.incremental;


--добавление в таблицу с метаданными информации о последней заливке данных
INSERT INTO ykir_meta
SELECT
     max(update_dt)
FROM
     ykir_target;

COMMIT;




--скрипт захвата инкремента из таблицы источника и заливка в таблицу-приемник
--1.Подготовка стейджинга
DELETE FROM
     ykir_stg;
DELETE FROM
     ykir_stg_delete;


--забираем из таблицы приемника новые строки для обработки update и delete
INSERT INTO ykir_stg
SELECT
     id
,    val
,    update_dt
FROM
     de11tm.incremental
WHERE
     update_dt > (
          SELECT
               max_update_date
          FROM
               ykir_metadata
     );


--заливаем все идентификаторы из таблицы incremental для удаления
INSERT INTO ykir_stg_delete
SELECT
     id
FROM
     de11tm.incremental;


--2.Обработка insert, update и delete в приемнике
--вставляем из стейджинговой таблицы в таблицу-приемник новые записи 
--(обработка insert)					   
INSERT INTO ykir_target
SELECT
     stg.id
,    stg.val
,    stg.update_dt
FROM
     ykir_stg stg
LEFT JOIN
     ykir_target tgt
          ON stg.id = tgt.id
WHERE
     tgt.id IS NULL;


--обновляем строки в таблице-приемнике по старым записям
--(обработка update)
UPDATE
     ykir_target targ
SET
     val = tmp.val
,    update_dt = tmp.update_dt
FROM
     (
          SELECT
               stg.id
          ,    stg.val
          ,    stg.update_dt
          FROM
               ykir_stg stg
          JOIN
               ykir_target tgt
                    ON stg.id = tgt.id
     ) tmp
WHERE
     targ.id = tmp.id;


--удаляем из таблицы-приемника записей, удаленных на источнике
--(обработка delete)
DELETE FROM
     de11tm.ykir_target
WHERE
     id IN (
          SELECT
               tgt.id
          FROM
               de11tm.ykir_target tgt
          LEFT JOIN
               de11tm.ykir_stg_delete stg
                    ON tgt.id = stg.id
          WHERE
               stg.id IS NULL
     );


--обновим метаданные
UPDATE
     ykir_metadata
SET
     max_update_date = (
          SELECT
               max(update_dt)
          FROM
               ykir_stg
     );


--и вот только теперь имеем право выполнить коммит
COMMIT;

TABLE de11tm.incremental;
