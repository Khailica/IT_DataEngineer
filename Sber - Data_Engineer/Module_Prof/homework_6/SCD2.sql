
--просмотр таблиц
TABLE de11tm.ykir_incremental;
TABLE de11tm.ykir_tfes_metadata;
TABLE de11tm.ykir_tfes_stg;
TABLE de11tm.ykir_tfes_stg_delete;
TABLE de11tm.ykir_tfes_target;
SELECT * FROM de11tm.ykir_tfes_target ORDER BY id, effective_from_dttm;



-- заливка новой строки в таблицу-источник
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



-- обновление строки в таблице-источнике
UPDATE
     de11tm.ykir_incremental
SET
     val = 'E'
,    update_dt = '2022-12-30 00:00:00'
WHERE
     id = 1 AND
     update_dt = '2022-12-29 17:35:15';

COMMIT;



-- удаление строки в таблице-источнике
DELETE FROM
     de11tm.ykir_incremental
WHERE
     id = 1 AND
     val = 'G' AND
     update_dt = '2022-12-27 20:00:18';

COMMIT;




--скрипт захвата инкремента из таблицы-источника и заливка в таблицу-приемник SCD2

--1.Подготовка стейджинга
DELETE FROM
     de11tm.ykir_tfes_stg;
DELETE FROM
     de11tm.ykir_tfes_stg_delete;



--забираем из таблицы-источника новые строки для обработки update
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
               max(max_update_dt)
          FROM
               de11tm.ykir_tfes_metadata
     );



--заливаем все идентификаторы из таблицы ykir_incremental для удаления
INSERT INTO de11tm.ykir_tfes_stg_delete(
     id
,    delete_dt
)
SELECT
     id
,    update_dt
FROM
     de11tm.ykir_incremental;




--2.Обработка insert, update и delete в приемнике

--вставляем из стейджинговой таблицы в таблицу-приемник SCD2 новые или изменённые записи 
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
     de11tm.ykir_tfes_stg AS stg;


          
--обновляем строки в таблице-приемнике SCD2 по старым записям
--(обработка update)
--1 вариант:
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


--2 вариант:
UPDATE
     de11tm.ykir_tfes_target AS targ
SET
     effective_to_dttm = tmp.update_dt - '1second'::interval
FROM
     (
          SELECT
               tgt.id
          ,    stg.update_dt
          ,    tgt.effective_from_dttm
          ,    tgt.effective_to_dttm
          FROM
               de11tm.ykir_tfes_target AS tgt
          JOIN
               de11tm.ykir_tfes_stg AS stg
                    ON stg.id = tgt.id
          WHERE
               tgt.effective_to_dttm = '5999-12-31 00:00:00' AND
               tgt.effective_from_dttm < stg.update_dt
     ) AS tmp
WHERE
     targ.id = tmp.id AND
     targ.effective_from_dttm = tmp.effective_from_dttm;




--вставляем удалённые на источнике данные в таблицу-приемник SCD2
--(обработка delete)
CREATE TEMPORARY TABLE row_del AS
     SELECT
          tgt.id
     ,    tgt.val
     ,    tgt.effective_from_dttm
     ,    current_timestamp time_delete
     ,    deleted_flg
     FROM
          de11tm.ykir_tfes_target AS tgt
     LEFT JOIN
          de11tm.ykir_tfes_stg_delete AS stg
               ON stg.id = tgt.id AND
               stg.delete_dt = tgt.effective_from_dttm
     WHERE
          stg.id IS NULL;


--обновляем effective_to_dttm удаляемой записи в таблице-приемнике SCD2
UPDATE
     de11tm.ykir_tfes_target AS targ
SET
     effective_to_dttm = tmp.time_delete - '1second'::interval
FROM
     (
          SELECT
               tgt.id
          ,    tgt.val
          ,    rd.time_delete
          ,    tgt.effective_from_dttm
          ,    tgt.effective_to_dttm
          FROM
               de11tm.ykir_tfes_target AS tgt
          JOIN
               row_del rd
                    ON rd.id = tgt.id
          WHERE
               rd.val = tgt.val AND
               tgt.effective_from_dttm < rd.time_delete
     ) AS tmp
WHERE
     targ.id = tmp.id AND
     targ.val = tmp.val AND
     targ.effective_from_dttm = tmp.effective_from_dttm;



--вставляем удаляемую запись в таблицу-приемник SCD2
INSERT INTO de11tm.ykir_tfes_target(
     id
,    val
,    effective_from_dttm
,    effective_to_dttm
,    deleted_flg
)
 SELECT
          id
     ,    val
     ,    time_delete
     ,    '5999-12-31 00:00:00'
     ,    1
     FROM
          row_del;

     

--добавление в таблицу с метаданными информации о последней заливке данных в таблицу-приемник SCD2
INSERT INTO de11tm.ykir_tfes_metadata
SELECT
     max(effective_from_dttm)
FROM
     de11tm.ykir_tfes_target;



--и вот только теперь имеем право выполнить коммит
COMMIT;



--просмотр таблиц
TABLE de11tm.ykir_incremental;
TABLE de11tm.ykir_tfes_metadata;
TABLE de11tm.ykir_tfes_stg;
TABLE de11tm.ykir_tfes_stg_delete;
TABLE de11tm.ykir_tfes_target;
SELECT * FROM de11tm.ykir_tfes_target ORDER BY id, effective_from_dttm;
