--DROP TABLE de11tm.ykir_incremental;
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


TABLE de11tm.ykir_incremental;


-- вставляем из консоли от имени клиента
\COPY de11tm.ykir_incremental(id, val, update_dt) FROM '/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/homework_6/ykir_incremental.csv' WITH DELIMITER ',' CSV HEADER;



--0.начальное решение (самая первая загрузка всего источника в приемник)

--создаем приемник
DROP TABLE ykir_tfes_target;
CREATE TABLE ykir_tfes_target(
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
CREATE TABLE ykir_tfes_stg(
     id
          integer
,    val
          varchar(10)
,    update_dt
          timestamp
);


--создаем таблица стейджинга для удаления записей
CREATE TABLE ykir_tfes_stg_delete(
     id
          integer
);


--создаем таблицу с мета-данными
CREATE TABLE ykir_tfes_metadata(
     max_update_dt
          timestamp  --максимальная дата update_dt среди загруженных в таблицу-приемник
);

TABLE de11tm.ykir_incremental;

 
--загрузка начального решения
--TRUNCATE ykir_tfes_target;
INSERT INTO ykir_tfes_target
SELECT
     id
,    val
,    update_dt
FROM
     de11tm.ykir_incremental;

TABLE ykir_tfes_target;


--добавление в таблицу с метаданными информации о последней заливке данных
INSERT INTO ykir_tfes_metadata
SELECT
     max(effective_from_dttm)
FROM
     ykir_tfes_target;
COMMIT;

TABLE ykir_tfes_metadata;



--скрипт захвата инкремента из таблицы источника и заливка в таблицу-приемник
--1.Подготовка стейджинга
	delete from ykir_tfes_stg;
	delete from ykir_tfes_stg_delete;

--забираем из таблицы приемника новые строки для обработки update и delete
insert into ykir_tfes_stg 
        Select id, val, update_dt 
        from de11tm.ykir_incremental
        Where UPDATE_DT > (Select max_update_date 
						   From ykir_ykir_tfes_metadata);					  
--заливаем все идентификаторы из таблицы ykir_incremental для удаления
insert into ykir_tfes_stg_delete select id from de11tm.ykir_incremental						  

--2.Обработка insert, update и delete в приемнике
--вставляем из стейджинговой таблицы в таблицу-приемник новые записи 
--(обработка insert)					   
insert into ykir_tfes_target 
	Select stg.id, stg.val, stg.update_dt  
	from ykir_tfes_stg stg
		left join ykir_tfes_target tgt
		on stg.ID = tgt.id 
	where tgt.id is NULL;


--обновляем строки в таблице-приемнике по старым записям
--(обработка update)
update ykir_tfes_target targ
	set val = tmp.val, 
	update_dt = tmp.update_dt
from (
	select 
		stg.ID, 
		stg.val, 
		stg.update_dt
	from ykir_tfes_stg stg
		inner join ykir_tfes_target tgt
		on stg.ID = tgt.ID) tmp
where targ.id = tmp.id;

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


select * from de11tm.ykir_incremental

--0.начальное решение (самая первая загрузка всего источника в приемник)

--создаем приемник
drop table ykir_tfes_target;
create table ykir_tfes_target
(ID int, 
VAL varchar(10), 
UPDATE_DT timestamp);

--создаем стейджинговую таблицу
create table ykir_tfes_stg
(ID int, 
VAL varchar(10), 
UPDATE_DT timestamp);

--создаем таблица стейджинга для удаления записей
create table ykir_tfes_stg_delete
(id int);

--создаем таблицу с мета-данными
create table ykir_ykir_tfes_metadata
(
max_update_dt timestamp --максимальная дата update_dt среди загруженных в таблицу-приемник 
);
 
--загрузка начального решения
insert into ykir_tfes_target select ID, VAL, UPDATE_DT from de11tm.ykir_incremental

--добавление в таблицу с метаданными информации о последней заливке данных
insert into ykir_tfes_meta values (Select max(UPDATE_DT) from ykir_tfes_target)
commit;



--скрипт захвата инкремента из таблицы источника и заливка в таблицу-приемник
--1.Подготовка стейджинга
	delete from ykir_tfes_stg;
	delete from ykir_tfes_stg_delete;

--забираем из таблицы приемника новые строки для обработки update и delete
insert into ykir_tfes_stg 
        Select id, val, update_dt 
        from de11tm.ykir_incremental
        Where UPDATE_DT > (Select max_update_date 
						   From ykir_ykir_tfes_metadata);					  
--заливаем все идентификаторы из таблицы ykir_incremental для удаления
insert into ykir_tfes_stg_delete select id from de11tm.ykir_incremental						  

--2.Обработка insert, update и delete в приемнике
--вставляем из стейджинговой таблицы в таблицу-приемник новые записи 
--(обработка insert)					   
insert into ykir_tfes_target 
	Select stg.id, stg.val, stg.update_dt  
	from ykir_tfes_stg stg
		left join ykir_tfes_target tgt
		on stg.ID = tgt.id 
	where tgt.id is NULL;


--обновляем строки в таблице-приемнике по старым записям
--(обработка update)
update ykir_tfes_target targ
	set val = tmp.val, 
	update_dt = tmp.update_dt
from (
	select 
		stg.ID, 
		stg.val, 
		stg.update_dt
	from ykir_tfes_stg stg
		inner join ykir_tfes_target tgt
		on stg.ID = tgt.ID) tmp
where targ.id = tmp.id;

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



 
--загрузка начального решения
insert into ykir_tfes_target select ID, VAL, UPDATE_DT from de11tm.ykir_incremental

--добавление в таблицу с метаданными информации о последней заливке данных
insert into ykir_tfes_meta values (Select max(UPDATE_DT) from ykir_tfes_target)
commit;



--скрипт захвата инкремента из таблицы источника и заливка в таблицу-приемник
--1.Подготовка стейджинга
	delete from ykir_tfes_stg;
	delete from ykir_tfes_stg_delete;

--забираем из таблицы приемника новые строки для обработки update и delete
insert into ykir_tfes_stg 
        Select id, val, update_dt 
        from de11tm.ykir_incremental
        Where UPDATE_DT > (Select max_update_date 
						   From ykir_ykir_tfes_metadata);					  
--заливаем все идентификаторы из таблицы ykir_incremental для удаления
insert into ykir_tfes_stg_delete select id from de11tm.ykir_incremental						  

--2.Обработка insert, update и delete в приемнике
--вставляем из стейджинговой таблицы в таблицу-приемник новые записи 
--(обработка insert)					   
insert into ykir_tfes_target 
	Select stg.id, stg.val, stg.update_dt  
	from ykir_tfes_stg stg
		left join ykir_tfes_target tgt
		on stg.ID = tgt.id 
	where tgt.id is NULL;


--обновляем строки в таблице-приемнике по старым записям
--(обработка update)
update ykir_tfes_target targ
	set val = tmp.val, 
	update_dt = tmp.update_dt
from (
	select 
		stg.ID, 
		stg.val, 
		stg.update_dt
	from ykir_tfes_stg stg
		inner join ykir_tfes_target tgt
		on stg.ID = tgt.ID) tmp
where targ.id = tmp.id;

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


