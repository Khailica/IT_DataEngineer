#!/usr/bin/python3

# %%
# импортируем библиотеки
import psycopg2 as ps
import pandas as pd
import os
import glob  # для работы cо списком файлов
import datetime

# указываем рабочий каталог
dir_path = '/home/de11tm/ykir/project/'


# %%
# Подключаемся к источнику - Database 'bank'
conn_src = ps.connect(
    host = 'de-edu-db.chronosavant.ru',
    port=  '5432',
    database= 'bank',
    user= 'bank_etl',
    password= 'bank_etl_password'
)


# %%
# Подключаемся к приемнику - Database 'edu'
conn_tgt = ps.connect(
    host = 'de-edu-db.chronosavant.ru',
    port=  '5432',
    database= 'edu',
    user= 'de11tm',
    password= 'samwisegamgee'
)


# %%
# Отключаем autocommit в Database
conn_src.autocommit = False
conn_tgt.autocommit = False


# %%
# Создаём курсоры к каждому соединению к Database
curs_src = conn_src.cursor()
curs_tgt = conn_tgt.cursor()


# %% [markdown]
# ## 2. Захват инкремента из источника и заливка в приёмник SCD2
# 
# - Обработка INSERT
# - Обработка UPDATE
# - Обработка DELETE
# 

# %% [markdown]
# #### 2.1 Для таблицы dim_accounts
# 

# %%
# подготовка стейджинга
curs_tgt.execute("""delete from de11tm.ykir_stg_dim_accounts""")
curs_tgt.execute("""delete from de11tm.ykir_stg_delete_dim_accounts""")

# забираем из метаданных дату последнего обновления таблицы
curs_tgt.execute("""SELECT
	max(max_update_dt)
FROM
	de11tm.ykir_meta_dim_accounts""")

# Записываем данные max_update_dt в переменную
date_max_update_dt = curs_tgt.fetchall()

# Забираем из таблицы-источника bank.info.accounts только новые строки для обработки
curs_src.execute("""SELECT
	account
,	valid_to
,	client
,	create_dt
,	update_dt
FROM
	info.accounts
WHERE
	update_dt > %s""", (date_max_update_dt))

# Записываем данные в переменную
res = curs_src.fetchall()

# Формируем датафрейм
names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns=names)

# Заливаем данные из dataframe в stg-таблицу
curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_dim_accounts(
	account
,	valid_to
,	client
,	create_dt
,	update_dt
)
VALUES(%s, %s, %s, %s, %s)""", df.values.tolist())


# Подготавливаю все идентификаторы из таблицы-источника для обработки удаления
curs_src.execute("""SELECT
	account
,	create_dt
,	update_dt	
FROM
	info.accounts""")

# Записываем данные в переменную
res = curs_src.fetchall()

# Формируем датафрейм
names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns=names)

# Заменяю начальные значения NaT в update_dt на значения из create_dt
df['update_dt'] = df['update_dt'].fillna(value=df['create_dt'])
df = df.drop(columns=['create_dt'])


# Заливаем все идентификаторы из таблицы-источника для обработки удаления
curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_delete_dim_accounts(
	account_num
,	delete_dt
)
VALUES(%s, %s)""", df.values.tolist())


# %%
# Обработка INSERT таблицы dim_accounts

# заливаем данные из stg-таблицы в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_dim_accounts_hist(
	account_num
,	valid_to
,	client
,	effective_from
,	effective_to
)
SELECT
	account
,	valid_to
,	client
,	create_dt
,	update_dt
FROM
	de11tm.ykir_stg_dim_accounts""")


# %%
# Обработка UPDATE таблицы dim_accounts

curs_tgt.execute("""UPDATE
	de11tm.ykir_dwh_dim_accounts_hist AS targ
SET
	effective_to = tmp.update_dt - '1second'::interval
FROM
	(
		SELECT
			tgt.account_num
		,	stg.update_dt
		,	tgt.effective_from
		,	tgt.effective_to
		FROM
			de11tm.ykir_dwh_dim_accounts_hist AS tgt
		JOIN
			de11tm.ykir_stg_dim_accounts AS stg
				ON tgt.account_num = stg.account
		WHERE
			tgt.effective_to = '5999-12-31 00:00:00' AND
			tgt.effective_from < stg.update_dt
	) AS tmp
WHERE
	targ.account_num = tmp.account_num AND
	targ.effective_from = tmp.effective_from""")


# %%
# Обработка DELETE таблицы dim_accounts
# вставляем удалённые на источнике данные в таблицу-приемник SCD2

# создаём временную таблицу для удобства
curs_tgt.execute("""CREATE TEMPORARY TABLE row_del_accounts AS
     SELECT
          tgt.account_num
     ,    tgt.valid_to
     ,    tgt.client
     ,    tgt.effective_from
     ,    current_timestamp AS time_delete
     ,    deleted_flg
     FROM
          de11tm.ykir_dwh_dim_accounts_hist AS tgt
     LEFT JOIN
          de11tm.ykir_stg_delete_dim_accounts AS stg
               ON tgt.account_num = stg.account_num AND
               stg.delete_dt = tgt.effective_from
     WHERE
          stg.account_num IS NULL""")

# обновляем effective_to удаляемой записи в таблице-приемнике SCD2
curs_tgt.execute("""UPDATE
     de11tm.ykir_dwh_dim_accounts_hist AS targ
SET
     effective_to = tmp.time_delete - '1second'::interval
FROM
     (
          SELECT
               tgt.account_num
          ,    rd.time_delete
          ,    tgt.effective_from
          ,    tgt.effective_to
          FROM
               de11tm.ykir_dwh_dim_accounts_hist AS tgt
          JOIN
               row_del_accounts AS rd
                    ON rd.account_num = tgt.account_num
          WHERE
               tgt.effective_from < rd.time_delete
     ) AS tmp
WHERE
     targ.account_num = tmp.account_num AND
     targ.effective_from = tmp.effective_from""")

# вставляем удаляемую запись в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_dim_accounts_hist(
	account_num
,	valid_to
,	client
,	effective_from
,	effective_to
,	deleted_flg
)
SELECT
	account_num
,	valid_to
,	client
,	time_delete
,	'5999-12-31 00:00:00'
,	1
FROM
	row_del_accounts""")


# %%
# Добавление в таблицу с метаданными информации о последней заливке данных в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_dim_accounts(
	max_update_dt
)
SELECT
     max(effective_from)
FROM
     de11tm.ykir_dwh_dim_accounts_hist""")


# %% [markdown]
# #### 2.2 Для таблицы dim_cards
# 

# %%
# подготовка стейджинга
curs_tgt.execute("""delete from de11tm.ykir_stg_dim_cards""")
curs_tgt.execute("""delete from de11tm.ykir_stg_delete_dim_cards""")

# забираем из метаданных дату последнего обновления таблицы
curs_tgt.execute("""SELECT
	max(max_update_dt)
FROM
	de11tm.ykir_meta_dim_cards""")

# Записываем данные max_update_dt в переменную
date_max_update_dt = curs_tgt.fetchall()

# Забираем из таблицы-источника bank.info.accounts только новые строки для обработки
curs_src.execute("""SELECT
	regexp_replace(card_num, '\s', '', 'g') AS card_num
,	account
,	create_dt
,	update_dt
FROM
	info.cards
WHERE
	update_dt > %s""", (date_max_update_dt))

# Записываем данные в переменную
res = curs_src.fetchall()

# Формируем датафрейм
names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns=names)

# Заливаем данные из dataframe в stg-таблицу
curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_dim_cards(
	card_num
,	account
,	create_dt
,	update_dt
)
VALUES(%s, %s, %s, %s)""", df.values.tolist())


# Подготавливаю все идентификаторы из таблицы-источника для обработки удаления
curs_src.execute("""SELECT
	regexp_replace(card_num, '\s', '', 'g') AS card_num
,	create_dt
,	update_dt	
FROM
	info.cards""")

# Записываем данные в переменную
res = curs_src.fetchall()

# Формируем датафрейм
names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns=names)

# Заменяю начальные значения NaT в update_dt на значения из create_dt
df['update_dt'] = df['update_dt'].fillna(value=df['create_dt'])
df = df.drop(columns=['create_dt'])


# Заливаем все идентификаторы из таблицы-источника для обработки удаления
curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_delete_dim_cards(
	card_num
,	delete_dt
)
VALUES(%s, %s)""", df.values.tolist())


# %%
# Обработка INSERT таблицы dim_cards

# заливаем данные из stg-таблицы в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_dim_cards_hist(
	card_num
,	account_num
,	effective_from
,	effective_to
)
SELECT
	card_num
,	account
,	create_dt
,	update_dt
FROM
	de11tm.ykir_stg_dim_cards""")


# %%
# Обработка UPDATE таблицы dim_cards

curs_tgt.execute("""UPDATE
	de11tm.ykir_dwh_dim_cards_hist AS targ
SET
	effective_to = tmp.update_dt - '1second'::interval
FROM
	(
		SELECT
			tgt.card_num
		,	stg.update_dt
		,	tgt.effective_from
		,	tgt.effective_to
		FROM
			de11tm.ykir_dwh_dim_cards_hist AS tgt
		JOIN
			de11tm.ykir_stg_dim_cards AS stg
				ON tgt.card_num = stg.card_num
		WHERE
			tgt.effective_to = '5999-12-31 00:00:00' AND
			tgt.effective_from < stg.update_dt
	) AS tmp
WHERE
	targ.card_num = tmp.card_num AND
	targ.effective_from = tmp.effective_from""")


# %%
# Обработка DELETE таблицы dim_cards
# вставляем удалённые на источнике данные в таблицу-приемник SCD2

# создаём временную таблицу для удобства
curs_tgt.execute("""CREATE TEMPORARY TABLE row_del_cards AS
     SELECT
          tgt.card_num
     ,    tgt.account_num
     ,    tgt.effective_from
     ,    current_timestamp AS time_delete
     ,    deleted_flg
     FROM
          de11tm.ykir_dwh_dim_cards_hist AS tgt
     LEFT JOIN
          de11tm.ykir_stg_delete_dim_cards AS stg
               ON tgt.card_num = stg.card_num AND
               stg.delete_dt = tgt.effective_from
     WHERE
          stg.card_num IS NULL""")

# обновляем effective_to удаляемой записи в таблице-приемнике SCD2
curs_tgt.execute("""UPDATE
     de11tm.ykir_dwh_dim_cards_hist AS targ
SET
     effective_to = tmp.time_delete - '1second'::interval
FROM
     (
          SELECT
               tgt.card_num
          ,    rd.time_delete
          ,    tgt.effective_from
          ,    tgt.effective_to
          FROM
               de11tm.ykir_dwh_dim_cards_hist AS tgt
          JOIN
               row_del_cards AS rd
                    ON rd.card_num = tgt.card_num
          WHERE
               tgt.effective_from < rd.time_delete
     ) AS tmp
WHERE
     targ.card_num = tmp.card_num AND
     targ.effective_from = tmp.effective_from""")

# вставляем удаляемую запись в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_dim_cards_hist(
	card_num
,	account_num
,	effective_from
,	effective_to
,	deleted_flg
)
SELECT
	card_num
,    account_num
,	time_delete
,	'5999-12-31 00:00:00'
,	1
FROM
	row_del_cards""")


# %%
# Добавление в таблицу с метаданными информации о последней заливке данных в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_dim_cards(
	max_update_dt
)
SELECT
     max(effective_from)
FROM
     de11tm.ykir_dwh_dim_cards_hist""")


# %% [markdown]
# #### 2.3 Для таблицы dim_clients
# 

# %%
# подготовка стейджинга
curs_tgt.execute("""delete from de11tm.ykir_stg_dim_clients""")
curs_tgt.execute("""delete from de11tm.ykir_stg_delete_dim_clients""")

# забираем из метаданных дату последнего обновления таблицы
curs_tgt.execute("""SELECT
	max(max_update_dt)
FROM
	de11tm.ykir_meta_dim_clients""")

# Записываем данные max_update_dt в переменную
date_max_update_dt = curs_tgt.fetchall()

# Забираем из таблицы-источника bank.info.accounts только новые строки для обработки
curs_src.execute("""SELECT
	client_id
,	last_name
,	first_name
,	patronymic
,	date_of_birth
,	passport_num
,	passport_valid_to
,	phone
,	create_dt
,	update_dt
FROM
	info.clients
WHERE
	update_dt > %s""", (date_max_update_dt))

# Записываем данные в переменную
res = curs_src.fetchall()

# Формируем датафрейм
names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns=names)

# Заливаем данные из dataframe в stg-таблицу
curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_dim_clients(
	client_id
,	last_name
,	first_name
,	patronymic
,	date_of_birth
,	passport_num
,	passport_valid_to
,	phone
,	create_dt
,	update_dt
)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", df.values.tolist())


# Подготавливаю все идентификаторы из таблицы-источника для обработки удаления
curs_src.execute("""SELECT
	client_id
,	create_dt
,	update_dt	
FROM
	info.clients""")

# Записываем данные в переменную
res = curs_src.fetchall()

# Формируем датафрейм
names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns=names)

# Заменяю начальные значения NaT в update_dt на значения из create_dt
df['update_dt'] = df['update_dt'].fillna(value=df['create_dt'])
df = df.drop(columns=['create_dt'])


# Заливаем все идентификаторы из таблицы-источника для обработки удаления
curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_delete_dim_clients(
	client_id
,	delete_dt
)
VALUES(%s, %s)""", df.values.tolist())


# %%
# Обработка INSERT таблицы dim_clients

# заливаем данные из stg-таблицы в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_dim_clients_hist(
	client_id
,	last_name
,	first_name
,	patronymic
,	date_of_birth
,	passport_num
,	passport_valid_to
,	phone
,	effective_from
,	effective_to
)
SELECT
	client_id
,	last_name
,	first_name
,	patronymic
,	date_of_birth
,	passport_num
,	passport_valid_to
,	phone
,	create_dt
,	update_dt
FROM
	de11tm.ykir_stg_dim_clients""")


# %%
# Обработка UPDATE таблицы dim_clients

curs_tgt.execute("""UPDATE
	de11tm.ykir_dwh_dim_clients_hist AS targ
SET
	effective_to = tmp.update_dt - '1second'::interval
FROM
	(
		SELECT
			tgt.client_id
		,	stg.update_dt
		,	tgt.effective_from
		,	tgt.effective_to
		FROM
			de11tm.ykir_dwh_dim_clients_hist AS tgt
		JOIN
			de11tm.ykir_stg_dim_clients AS stg
				ON tgt.client_id = stg.client_id
		WHERE
			tgt.effective_to = '5999-12-31 00:00:00' AND
			tgt.effective_from < stg.update_dt
	) AS tmp
WHERE
	targ.client_id = tmp.client_id AND
	targ.effective_from = tmp.effective_from""")


# %%
# Обработка DELETE таблицы dim_clients
# вставляем удалённые на источнике данные в таблицу-приемник SCD2

# создаём временную таблицу для удобства
curs_tgt.execute("""CREATE TEMPORARY TABLE row_del_clients AS
     SELECT
          tgt.client_id
     ,    tgt.last_name
     ,    tgt.first_name
     ,    tgt.patronymic
     ,    tgt.date_of_birth
     ,    tgt.passport_num
     ,    tgt.passport_valid_to
     ,    tgt.phone
     ,    tgt.effective_from
     ,    current_timestamp AS time_delete
     ,    deleted_flg
     FROM
          de11tm.ykir_dwh_dim_clients_hist AS tgt
     LEFT JOIN
          de11tm.ykir_stg_delete_dim_clients AS stg
               ON tgt.client_id = stg.client_id AND
               stg.delete_dt = tgt.effective_from
     WHERE
          stg.client_id IS NULL""")

# обновляем effective_to удаляемой записи в таблице-приемнике SCD2
curs_tgt.execute("""UPDATE
     de11tm.ykir_dwh_dim_clients_hist AS targ
SET
     effective_to = tmp.time_delete - '1second'::interval
FROM
     (
          SELECT
               tgt.client_id
          ,    rd.time_delete
          ,    tgt.effective_from
          ,    tgt.effective_to
          FROM
               de11tm.ykir_dwh_dim_clients_hist AS tgt
          JOIN
               row_del_clients AS rd
                    ON rd.client_id = tgt.client_id
          WHERE
               tgt.effective_from < rd.time_delete
     ) AS tmp
WHERE
     targ.client_id = tmp.client_id AND
     targ.effective_from = tmp.effective_from""")

# вставляем удаляемую запись в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_dim_clients_hist(
	client_id
,	last_name
,	first_name
,	patronymic
,	date_of_birth
,	passport_num
,	passport_valid_to
,	phone
,	effective_from
,	effective_to
,	deleted_flg
)
SELECT
	client_id
,	last_name
,	first_name
,	patronymic
,	date_of_birth
,	passport_num
,	passport_valid_to
,	phone
,	time_delete
,	'5999-12-31 00:00:00'
,	1
FROM
	row_del_clients""")


# %%
# Добавление в таблицу с метаданными информации о последней заливке данных в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_dim_clients(
	max_update_dt
)
SELECT
     max(effective_from)
FROM
     de11tm.ykir_dwh_dim_clients_hist""")


# %% [markdown]
# #### 2.4 Для таблицы dim_terminals
# 

# %%
# подготовка стейджинга
curs_tgt.execute("""delete from de11tm.ykir_stg_dim_terminals""")
curs_tgt.execute("""delete from de11tm.ykir_stg_delete_dim_terminals""")

# забираем из метаданных дату последнего обновления таблицы
curs_tgt.execute("""SELECT
	max(max_update_dt)
FROM
	de11tm.ykir_meta_dim_terminals""")

# Записываем данные max_update_dt в переменную
date_max_update_dt = curs_tgt.fetchall()


# найдём файл terminals_NNNNNNNN.xlsx в каталоге
filename = glob.glob(dir_path + 'data/terminals_*')

# преобразование списка в строку
filename = ''.join(filename)

# обрабатываем исключение, если файла в каталоге нет или их несколько
try:
	# формируем dataframe из Excel-файла
	df = pd.read_excel(filename)

	# получение даты из имени файла
	date = datetime.datetime.strptime(filename[-13:-5], '%d%m%Y').date()

	# добавление поле с датой в dataframe
	df.insert(4, "date_file", date)

	# Заливаем данные из dataframe в stg-таблицу
	curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_dim_terminals(
			terminal_id
		,	terminal_type
		,	terminal_city
		,	terminal_address
		,	date_file
		)
		VALUES(%s, %s, %s, %s, %s)""", df.values.tolist())
	

	# Подготавливаю все идентификаторы из таблицы-источника для обработки удаления
	curs_tgt.execute("""SELECT
		terminal_id
	,	date_file	
	FROM
		de11tm.ykir_stg_dim_terminals""")

	# Записываем данные в переменную
	res = curs_src.fetchall()

	# Формируем датафрейм
	names = [name[0] for name in curs_src.description]
	df = pd.DataFrame(res, columns=names)


	# Заливаем все идентификаторы из таблицы-источника для обработки удаления
	curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_delete_dim_terminals(
		terminal_id
	,	delete_dt
	)
	VALUES(%s, %s)""", df.values.tolist())



	# Обработка INSERT таблицы dim_terminals

	# заливаем данные из stg-таблицы в таблицу-приемник SCD2
	curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_dim_terminals_hist(
			terminal_id
		,	terminal_type
		,	terminal_city
		,	terminal_address
		,	effective_from
		,	effective_to
		)
		SELECT
			terminal_id
		,	terminal_type
		,	terminal_city
		,	terminal_address
		,	date_file::timestamp AS effective_from
		,	coalesce(
				lead(date_file) OVER(PARTITION BY terminal_id ORDER BY date_file) - '1 second'::interval
			,	'5999-12-31 00:00:00'
			) AS effective_to
		FROM
			de11tm.ykir_stg_dim_terminals""")



	# Обработка UPDATE таблицы dim_terminals

	curs_tgt.execute("""UPDATE
		de11tm.ykir_dwh_dim_terminals_hist AS targ
	SET
		effective_to = tmp.date_file - '1second'::interval
	FROM
		(
			SELECT
				tgt.terminal_id
			,	stg.date_file
			,	tgt.effective_from
			,	tgt.effective_to
			FROM
				de11tm.ykir_dwh_dim_terminals_hist AS tgt
			JOIN
				de11tm.ykir_stg_dim_terminals AS stg
					ON tgt.terminal_id = stg.terminal_id
			WHERE
				tgt.effective_to = '5999-12-31 00:00:00' AND
				tgt.effective_from < stg.date_file
		) AS tmp
	WHERE
		targ.terminal_id = tmp.terminal_id AND
		targ.effective_from = tmp.effective_from""")



	# Обработка DELETE таблицы dim_terminals
	# вставляем удалённые на источнике данные в таблицу-приемник SCD2

	# создаём временную таблицу для удобства
	curs_tgt.execute("""CREATE TEMPORARY TABLE row_del_terminals AS
		SELECT
			tgt.terminal_id
		,    tgt.terminal_type
		,    tgt.terminal_city
		,    terminal_address
		,    tgt.effective_from
		,    current_timestamp AS time_delete
		,    deleted_flg
		FROM
			de11tm.ykir_dwh_dim_terminals_hist AS tgt
		LEFT JOIN
			de11tm.ykir_stg_delete_dim_terminals AS stg
				ON tgt.terminal_id = stg.terminal_id AND
				stg.delete_dt = tgt.effective_from
		WHERE
			stg.terminal_id IS NULL""")

	# обновляем effective_to удаляемой записи в таблице-приемнике SCD2
	curs_tgt.execute("""UPDATE
		de11tm.ykir_dwh_dim_terminals_hist AS targ
	SET
		effective_to = tmp.time_delete - '1second'::interval
	FROM
		(
			SELECT
				tgt.terminal_id
			,    rd.time_delete
			,    tgt.effective_from
			,    tgt.effective_to
			FROM
				de11tm.ykir_dwh_dim_terminals_hist AS tgt
			JOIN
				row_del_terminals AS rd
						ON rd.terminal_id = tgt.terminal_id
			WHERE
				tgt.effective_from < rd.time_delete
		) AS tmp
	WHERE
		targ.terminal_id = tmp.terminal_id AND
		targ.effective_from = tmp.effective_from""")

	# вставляем удаляемую запись в таблицу-приемник SCD2
	curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_dim_terminals_hist(
		terminal_id
	,	terminal_type
	,	terminal_city
	,	terminal_address
	,	effective_from
	,	effective_to
	,	deleted_flg
	)
	SELECT
		terminal_id
	,	terminal_type
	,	terminal_city
	,	terminal_address
	,	time_delete
	,	'5999-12-31 00:00:00'
	,	1
	FROM
		row_del_terminals""")


	# добавление в таблицу с метаданными информации о последней заливке данных из stg в таблицу-приемник SCD2
	curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_dim_terminals(
		max_update_dt
	)
	SELECT
		max(date_file) max_update_dt
	FROM
		de11tm.ykir_stg_dim_terminals""")

	# Заархивируем использованный файл

	# название архивного файла
	name_archive = filename[-23:-5] + '.backup'

	# полный путь к архивному файлу
	dst_dir = dir_path + 'archive/' + name_archive

	# перенесём файл в архив
	os.rename(filename, dst_dir)
	
except FileNotFoundError as e: 
	# Открываем файл, чтобы записать сообщение
	with open(dir_path + 'error_log.txt', 'a') as f:
		# 1-я строка - timestamp (время)
		f.write(str(datetime.datetime.now()) + '\n')
		# 2-я строка - type of error (type of exception)
		f.write(str(type(e)) + '\n')
		# 3-я строка - error message (message of exception)
		f.write('файла terminals_NNNNNNNN.xlsx в каталоге нет' + '\n')
		# 4-я строкa - separator (для "краcоты")
		f.write('-'*50 + '\n')
except NotADirectoryError as e:
	with open(dir_path + 'error_log.txt', 'a') as f:
		f.write(str(datetime.datetime.now()) + '\n')
		f.write(str(type(e)) + '\n')
		f.write('больше одного файла terminals_NNNNNNNN.xlsx в каталоге' + '\n')
		f.write('-'*50 + '\n')

# %% [markdown]
# #### 2.5 Для таблицы fact_passport_blacklist
# 

# %%
# найдём файл passport_blacklist_NNNNNNNN.xlsx в каталоге
filename = glob.glob(dir_path + 'data/passport_blacklist_*')

# преобразование списка в строку
filename = ''.join(filename)

# обрабатываем исключение, если файла в каталоге нет или их несколько
try:
	# формируем dataframe из Excel-файла
	df = pd.read_excel(filename)

	# Заливаем данные из dataframe в stg-таблицу
	curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_fact_passport_blacklist(
		"date"
	,	passport
	)
	VALUES(%s, %s)""", df.values.tolist())

	# Заливаем данные из stg-таблицы в таблицу-приемник SCD2
	curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_fact_passport_blacklist(
		passport_num
	,	entry_dt
	)
	SELECT
		passport	
	,	"date"
	FROM
		de11tm.ykir_stg_fact_passport_blacklist""")


	# добавление в таблицу с метаданными информации о последней заливке данных из stg в таблицу-приемник SCD2
	curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_fact_passport_blacklist(
		max_update_dt
	)
	SELECT
		max("date") AS max_update_dt
	FROM
		de11tm.ykir_stg_fact_passport_blacklist""")


	# Заархивируем использованный файл

	# название архивного файла
	name_archive =filename[-32:-5] + '.backup'

	# полный путь к архивному файлу
	dst_dir = dir_path + 'archive/' + name_archive

	# перенесём файл в архив
	os.rename(filename, dst_dir)
	
except FileNotFoundError as e: 
	# Открываем файл, чтобы записать сообщение
	with open(dir_path + 'error_log.txt', 'a') as f:
		# 1-я строка - timestamp (время)
		f.write(str(datetime.datetime.now()) + '\n')
		# 2-я строка - type of error (type of exception)
		f.write(str(type(e)) + '\n')
		# 3-я строка - error message (message of exception)
		f.write('файла passport_blacklist_NNNNNNNN.xlsx в каталоге нет' + '\n')
		# 4-я строкa - separator (для "краcоты")
		f.write('-'*50 + '\n')
except NotADirectoryError as e:
	with open(dir_path + 'error_log.txt', 'a') as f:
		f.write(str(datetime.datetime.now()) + '\n')
		f.write(str(type(e)) + '\n')
		f.write('больше одного файла passport_blacklist_NNNNNNNN.xlsx в каталоге' + '\n')
		f.write('-'*50 + '\n')

# %% [markdown]
# #### 2.6 Для таблицы fact_transactions
# 

# %%
# Начальная загрузка для fact_transactions

# найдём файл transactions_NNNNNNNN.xlsx в каталоге
filename = glob.glob(dir_path + 'data/transactions_*')

# преобразование списка в строку
filename = ''.join(filename)

# обрабатываем исключение, если файла в каталоге нет или их несколько
try:
    # формируем dataframe из Excel-файла
    df = pd.read_csv(filename, sep=';', decimal=',')

    # Заливаем данные из dataframe в stg-таблицу
    curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_fact_transactions(
		transaction_id
	,	transaction_date
	,	amount
	,	card_num
	,	oper_type
	,	oper_result
	,	terminal
	)
	VALUES(%s, cast(%s AS timestamp), %s, %s, %s, %s, %s)""", df.values.tolist())

    # Заливаем данные из stg-таблицы в таблицу-приемник SCD2
    curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_fact_transactions(
		trans_id
	,	trans_date
	,	card_num
	,	oper_type
	,	amt
	,	oper_result
	,	terminal
	)
	SELECT
		transaction_id
	,	transaction_date
	,	regexp_replace(card_num, '\s', '', 'g') AS card_num
	,	oper_type
	,	amount
	,	oper_result
	,	terminal
	FROM
		de11tm.ykir_stg_fact_transactions""")

    # добавление в таблицу с метаданными информации о последней заливке данных из stg в таблицу-приемник SCD2
    curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_fact_transactions(
		max_update_dt
	)
	SELECT
		max(transaction_date) AS max_update_dt
	FROM
		de11tm.ykir_stg_fact_transactions""")

    # Заархивируем использованный файл

    # название архивного файла
    name_archive = filename[-25:-5] + '.backup'

    # полный путь к архивному файлу
    dst_dir = dir_path + 'archive/' + name_archive

    # перенесём файл в архив
    os.rename(filename, dst_dir)

except FileNotFoundError as e:
    # Открываем файл, чтобы записать сообщение
    with open(dir_path + 'error_log.txt', 'a') as f:
        # 1-я строка - timestamp (время)
        f.write(str(datetime.datetime.now()) + '\n')
        # 2-я строка - type of error (type of exception)
        f.write(str(type(e)) + '\n')
        # 3-я строка - error message (message of exception)
        f.write('файла transactions_NNNNNNNN.xlsx в каталоге нет' + '\n')
        # 4-я строкa - separator (для "краcоты")
        f.write('-'*50 + '\n')
except NotADirectoryError as e:
    with open(dir_path + 'error_log.txt', 'a') as f:
        f.write(str(datetime.datetime.now()) + '\n')
        f.write(str(type(e)) + '\n')
        f.write('больше одного файла transactions_NNNNNNNN.xlsx в каталоге' + '\n')
        f.write('-'*50 + '\n')


# %% [markdown]
# ## 3. Создание отчёта

# %%
# 1. Совершение операции при просроченном или заблокированном паспорте
curs_tgt.execute("""INSERT INTO de11tm.ykir_rep_fraud(
	event_dt
,	passport
,	fio
,	phone
,	event_type
,	report_dt
)
SELECT
	trans.trans_date AS event_dt
,	clients.passport_num AS passport
,	concat(
		last_name
	,	' '
	,	first_name
	,	' '
	,	patronymic
	) AS fio
,	clients.phone
,	'1' AS event_type
,	(
		SELECT
			max(trans_date)::date
		FROM
			de11tm.ykir_dwh_fact_transactions
	) AS report_dt
FROM
	de11tm.ykir_dwh_fact_transactions AS trans
JOIN
	de11tm.ykir_dwh_dim_cards_hist AS cards
		ON trans.card_num = cards.card_num
JOIN
	de11tm.ykir_dwh_dim_accounts_hist AS accounts
		ON cards.account_num = accounts.account_num
JOIN
	de11tm.ykir_dwh_dim_clients_hist AS clients
		ON accounts.client = clients.client_id
WHERE
	clients.passport_valid_to < trans.trans_date OR
	clients.passport_num IN (
		SELECT
			passport_num
		FROM
			de11tm.ykir_dwh_fact_passport_blacklist
	)""")

# %%
# 2. Совершение операции при недействующем договоре
curs_tgt.execute("""INSERT INTO de11tm.ykir_rep_fraud(
     event_dt
,    passport
,    fio
,    phone
,    event_type
,    report_dt
)
SELECT
     trans.trans_date AS event_dt
,    clients.passport_num AS passport
,    concat(
          last_name
     ,    ' '
     ,    first_name
     ,    ' '
     ,    patronymic
     ) AS fio
,    clients.phone
,    '2' AS event_type
,    (
          SELECT
               max(trans_date)::date
          FROM
               de11tm.ykir_dwh_fact_transactions
     ) AS report_dt
FROM
     de11tm.ykir_dwh_fact_transactions AS trans
JOIN
     de11tm.ykir_dwh_dim_cards_hist AS cards
          ON trans.card_num = cards.card_num
JOIN
     de11tm.ykir_dwh_dim_accounts_hist AS accounts
          ON cards.account_num = accounts.account_num
JOIN
     de11tm.ykir_dwh_dim_clients_hist AS clients
          ON accounts.client = clients.client_id
WHERE
     trans.oper_result = 'SUCCESS' AND
     (
          clients.passport_valid_to < trans.trans_date::date OR
          accounts.valid_to < trans.trans_date::date
     )""")

# %%
# 3. Совершение операций в разных городах в течение одного часа 
curs_tgt.execute("""INSERT INTO de11tm.ykir_rep_fraud(
     event_dt
,    passport
,    fio
,    phone
,    event_type
,    report_dt
)
WITH cte AS (
     SELECT
          trans.trans_date AS event_dt
     ,    clients.passport_num AS passport
     ,    concat(
               last_name
          ,    ' '
          ,    first_name
          ,    ' '
          ,    patronymic
          ) AS fio
     ,    clients.phone
     ,    '3' event_type
     ,    (
               SELECT
                    max(trans_date)::date
               FROM
                    de11tm.ykir_dwh_fact_transactions
          ) AS report_dt
     ,    term.terminal_city
     ,    lag(term.terminal_city) OVER(PARTITION BY trans.card_num ORDER BY trans.trans_date) AS prev_city
     ,    trans.trans_date - lag(trans.trans_date) OVER(PARTITION BY trans.card_num ORDER BY trans.trans_date) AS dt_diff
     FROM
          de11tm.ykir_dwh_fact_transactions AS trans
     LEFT JOIN
          de11tm.ykir_dwh_dim_terminals_hist AS term
               ON trans.terminal = term.terminal_id
     LEFT JOIN
          de11tm.ykir_dwh_dim_cards_hist AS cards
               ON trans.card_num = cards.card_num
     LEFT JOIN
          de11tm.ykir_dwh_dim_accounts_hist AS accounts
               ON cards.account_num = accounts.account_num
     LEFT JOIN
          de11tm.ykir_dwh_dim_clients_hist clients
               ON accounts.client = clients.client_id
)
SELECT
     event_dt
,    passport
,    fio
,    phone
,    event_type
,    report_dt
FROM
     cte
WHERE
     terminal_city <> prev_city AND
     dt_diff <= '01:00:00'""")

# %%
# 4. Попытка подбора суммы.
# В течение 20 минут проходит более 3х операций со следующим шаблоном – каждая последующая меньше предыдущей,
# при этом отклонены все кроме последней. Последняя операция (успешная) в такой цепочке считается мошеннической.

# %%
# выполняем транзакцию
conn_tgt.commit()

# %%
# закрываем соединения
conn_src.close()
conn_tgt.close()



