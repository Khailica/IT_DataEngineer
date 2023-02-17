#!/usr/bin/python3

# %%
# импортируем библиотеки
import psycopg2 as ps
import pandas as pd
import os
import glob #для работы cо списком файлов
import datetime

# указываем рабочий каталог
# dir_path = '/home/de11tm/ykir/project/'
dir_path = '/Users/frank/Documents/LEARNING IT, Eng, עברית/IT Data Engineer/Courses/Sber - Data_Engineer/Module_Prof/final_project/'

# %%
# Подключаемся к источнику - Database 'bank'
conn_src = ps.connect(host = 'de-edu-db.chronosavant.ru',
                        port=  '5432',
                        database= 'bank',
                        user= 'bank_etl',
                        password= 'bank_etl_password')

# %%
# Подключаемся к приемнику - Database 'edu'
# conn_tgt = ps.connect(
#     host = 'de-edu-db.chronosavant.ru',
#     port=  '5432',
#     database= 'edu',
#     user= 'de11tm',
#     password= 'samwisegamgee'
# )
conn_tgt = ps.connect(
    host = 'localhost',
    port=  '5432',
    database= 'postgres',
    user= 'postgres',
    password= 'penthous'
)

# %%
# Отключаем autocommit в Database
conn_src.autocommit = False
conn_tgt.autocommit = False

# %%
# Создаём курсоры к каждому соединению к Database
curs_src = conn_src.cursor()
curs_tgt = conn_tgt.cursor()

# %%
# Очищаем stg таблички
curs_tgt.execute("""delete from de11tm.ykir_stg_dim_accounts""")
curs_tgt.execute("""delete from de11tm.ykir_stg_dim_cards""")
curs_tgt.execute("""delete from de11tm.ykir_stg_dim_clients""")
curs_tgt.execute("""delete from de11tm.ykir_stg_dim_terminals""")
curs_tgt.execute("""delete from de11tm.ykir_stg_fact_passport_blacklist""")
curs_tgt.execute("""delete from de11tm.ykir_stg_fact_transactions""")

# %% [markdown]
# ### 2. Захват данных из источника в STG и начальная загрузка в хранилище DWH
# 

# %%
# Начальная загрузка для dim_accounts

# Чтение из источника bank.info.accounts
curs_src.execute("""SELECT
	account
,	valid_to
,	client
,	create_dt
,	update_dt
FROM 
	info.accounts""")

# Записываем данные в переменную
res = curs_src.fetchall()

# Формируем датафрейм
names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns = names)

# Заливаем данные из dataframe в stg-таблицу
curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_dim_accounts(
	account
,	valid_to
,	client
,	create_dt
,	update_dt
)
VALUES(%s, %s, %s, %s, %s)""", df.values.tolist())

# Заливаем данные из stg-таблицы в таблицу-приемник SCD2
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
,	coalesce(
		lead(update_dt) OVER(PARTITION BY account ORDER BY update_dt) - '1 second'::interval
	,	'5999-12-31 00:00:00'
	) AS effective_to
FROM
	de11tm.ykir_stg_dim_accounts""")


# Добавляем в таблицу с метаданными информации о последней заливке данных из stg в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_dim_accounts(
	max_update_dt
)
SELECT
	max(coalesce(create_dt, update_dt)) AS max_update_dt
FROM
	de11tm.ykir_stg_dim_accounts""")

# %%
# Начальная загрузка для dim_cards

# Чтение из источника bank.info.cards
curs_src.execute("""SELECT
	card_num
,	account
,	create_dt
,	update_dt
FROM 
	info.cards""")

# Записываем данные в переменную
res = curs_src.fetchall()

# Формируем датафрейм
names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns = names)

# Заливаем данные из dataframe в stg-таблицу
curs_tgt.executemany("""INSERT INTO de11tm.ykir_stg_dim_cards(
	card_num
,	account
,	create_dt
,	update_dt
)
VALUES(%s, %s, %s, %s)""", df.values.tolist())

# Заливаем данные из stg-таблицы в таблицу-приемник SCD2
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
,	coalesce(
		lead(update_dt) OVER(PARTITION BY card_num ORDER BY update_dt) - '1 second'::interval
	,	'5999-12-31 00:00:00'
	) AS effective_to
FROM
	de11tm.ykir_stg_dim_cards""")


# добавление в таблицу с метаданными информации о последней заливке данных из stg в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_dim_cards(
	max_update_dt
)
SELECT
	max(coalesce(create_dt, update_dt)) AS max_update_dt
FROM
	de11tm.ykir_stg_dim_cards""")

# %%
# Начальная загрузка для dim_clients

# Чтение из источника bank.info.clients
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
	info.clients""")

# Записываем данные в переменную
res = curs_src.fetchall()

# Формируем датафрейм
names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns = names)

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

# Заливаем данные из stg-таблицы в таблицу-приемник SCD2
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
,	coalesce(
		lead(update_dt) OVER(PARTITION BY client_id ORDER BY update_dt) - '1 second'::interval
	,	'5999-12-31 00:00:00'
	) AS effective_to
FROM
	de11tm.ykir_stg_dim_clients""")


# добавление в таблицу с метаданными информации о последней заливке данных из stg в таблицу-приемник SCD2
curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_dim_clients(
	max_update_dt
)
SELECT
	max(coalesce(create_dt, update_dt)) AS max_update_dt
FROM
	de11tm.ykir_stg_dim_clients""")

# %%
# Начальная загрузка для dim_terminals

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

	# Заливаем данные из stg-таблицы в таблицу-приемник SCD2
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


	# добавление в таблицу с метаданными информации о последней заливке данных из stg в таблицу-приемник SCD2
	curs_tgt.execute("""INSERT INTO de11tm.ykir_meta_dim_terminals(
		max_update_dt
	)
	SELECT
		max(date_file) AS max_update_dt
	FROM
		de11tm.ykir_stg_dim_terminals""")


	# Заархивируем использованный файл

	# название архивного файла
	name_archive =filename[-23:-5] + '.backup'

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

# %%
# Начальная загрузка для fact_passport_blacklist

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
	curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_fact_passport_blacklist_hist(
		passport_num
	,	entry_dt
	,	effective_from
	,	effective_to
	)
	SELECT
		passport	
	,	"date"
	,	"date"::timestamp AS effective_from
	,	coalesce(
			lead("date") OVER(PARTITION BY passport ORDER BY "date") - '1 second'::interval
		,	'5999-12-31 00:00:00'
		) AS effective_to
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

# %%
# Начальная загрузка для fact_transactions

# найдём файл transactions_NNNNNNNN.xlsx в каталоге
filename = glob.glob(dir_path + 'data/transactions_*')

# преобразование списка в строку
filename = ''.join(filename)

# обрабатываем исключение, если файла в каталоге нет или их несколько
try:
	# формируем dataframe из Excel-файла
	df = pd.read_csv(filename,sep = ';' )

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
	VALUES(%s, %s, %s, %s, %s, %s, %s)""", df.values.tolist())

	# Заливаем данные из stg-таблицы в таблицу-приемник SCD2
	curs_tgt.execute("""INSERT INTO de11tm.ykir_dwh_fact_transactions_hist(
		trans_id
	,	trans_date
	,	card_num
	,	oper_type
	,	amt
	,	oper_result
	,	terminal
	,	effective_from
	,	effective_to
	)
	SELECT
		transaction_id
	,	transaction_date
	,	card_num
	,	oper_type
	,	amount
	,	oper_result
	,	terminal
	,	transaction_date::timestamp AS effective_from
	,	coalesce(
			lead(transaction_date) OVER(PARTITION BY transaction_id ORDER BY transaction_date) - '1 second'::interval
		,	'5999-12-31 00:00:00'
		) AS effective_to
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
	name_archive =filename[-25:-5] + '.backup'

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

# %%
# выполняем транзакцию
conn_tgt.commit()

# %%
# закрываем соединения
conn_src.close()
conn_tgt.close()


