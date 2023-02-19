-- 1.Создаём таблицы измерений в приёмнике:

DROP TABLE IF EXISTS de11tm.ykir_dwh_dim_accounts_hist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_dwh_dim_accounts_hist(
     account_num
          varchar(20)
,    valid_to
          date
,    client
          varchar(10)
,    effective_from
          timestamp
,    effective_to
          timestamp
               DEFAULT '5999-12-31 00:00:00'
,    deleted_flg
          smallint
               DEFAULT 0
);


DROP TABLE IF EXISTS de11tm.ykir_dwh_dim_cards_hist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_dwh_dim_cards_hist(
     card_num
          varchar(20)
,    account_num
          varchar(20)
,    effective_from
          timestamp
,    effective_to
          timestamp
               DEFAULT '5999-12-31 00:00:00'
,    deleted_flg
          smallint
               DEFAULT 0
);


DROP TABLE IF EXISTS de11tm.ykir_dwh_dim_clients_hist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_dwh_dim_clients_hist(
     client_id
          varchar(10)
,    last_name
          varchar(20)
,    first_name
          varchar(20)
,    patronymic
          varchar(20)
,    date_of_birth
          date
,    passport_num
          varchar(15)
,    passport_valid_to
          date
,    phone
          varchar(16)
,    effective_from
          timestamp
,    effective_to
          timestamp
               DEFAULT '5999-12-31 00:00:00'
,    deleted_flg
          smallint
               DEFAULT 0
);


DROP TABLE IF EXISTS de11tm.ykir_dwh_dim_terminals_hist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_dwh_dim_terminals_hist(
     terminal_id
          varchar(10)
,    terminal_type
          varchar(5)
,    terminal_city
          varchar(30)
,    terminal_address
          varchar(100)
,    effective_from
          timestamp
,    effective_to
          timestamp
               DEFAULT '5999-12-31 00:00:00'
,    deleted_flg
          smallint
               DEFAULT 0
);



-- 2.Создаём таблицы фактов в приёмнике:

DROP TABLE IF EXISTS de11tm.ykir_dwh_fact_passport_blacklist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_dwh_fact_passport_blacklist(
     passport_num
          varchar(15)
,    entry_dt
          date
);


DROP TABLE IF EXISTS de11tm.ykir_dwh_fact_transactions;
CREATE TABLE IF NOT EXISTS de11tm.ykir_dwh_fact_transactions(
     trans_id
          varchar(20)
,    trans_date
          timestamp
,    card_num
          varchar(20)
,    oper_type
          varchar(10)
,    amt
          varchar(15)
,    oper_result
          varchar(10)
,    terminal
          varchar(10)
);

       

     
-- 3.Создаём стейджинговые таблицы:
          
DROP TABLE IF EXISTS de11tm.ykir_stg_dim_accounts;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_dim_accounts(
     account
          varchar(20)
,    valid_to
          date
,    client
          varchar(10)
,    create_dt
          timestamp
,    update_dt
          timestamp
);
  

DROP TABLE IF EXISTS de11tm.ykir_stg_dim_cards;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_dim_cards(
     card_num
          varchar(20)
,    account
          varchar(20)
,    create_dt
          timestamp
,    update_dt
          timestamp
);


DROP TABLE IF EXISTS de11tm.ykir_stg_dim_clients;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_dim_clients(
     client_id
          varchar(10)
,    last_name
          varchar(20)
,    first_name
          varchar(20)
,    patronymic
          varchar(20)
,    date_of_birth
          date
,    passport_num
          varchar(15)
,    passport_valid_to
          date
,    phone
          varchar(16)
,    create_dt
          timestamp
,    update_dt
          timestamp
);


DROP TABLE IF EXISTS de11tm.ykir_stg_dim_terminals;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_dim_terminals(
     terminal_id
          varchar(10)
,    terminal_type
          varchar(5)
,    terminal_city
          varchar(30)
,    terminal_address
          varchar(100)
,    date_file
          date
);


DROP TABLE IF EXISTS de11tm.ykir_stg_fact_passport_blacklist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_fact_passport_blacklist(
     "date" 
          date
,    passport
          varchar(15)
);


DROP TABLE IF EXISTS de11tm.ykir_stg_fact_transactions;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_fact_transactions(
     transaction_id
          varchar(20)
,    transaction_date
          timestamp
,    amount
          varchar(15)
,    card_num
          varchar(20)
,    oper_type
          varchar(10)
,    oper_result
          varchar(10)
,    terminal
          varchar(10)
);



-- 4. Создаём стейджинг-таблицы для обработки удаления:

DROP TABLE IF EXISTS de11tm.ykir_stg_delete_dim_accounts;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_delete_dim_accounts(
     account_num
          varchar(20)
,    delete_dt
          timestamp
);

DROP TABLE IF EXISTS de11tm.ykir_stg_delete_dim_cards;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_delete_dim_cards(
     card_num
          varchar(20)
,    delete_dt
          timestamp
);

DROP TABLE IF EXISTS de11tm.ykir_stg_delete_dim_clients;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_delete_dim_clients(
     client_id
          varchar(10)
,    delete_dt
          timestamp
);

DROP TABLE IF EXISTS de11tm.ykir_stg_delete_dim_terminals;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_delete_dim_terminals(
     terminal_id
          varchar(10)
,    delete_dt
          timestamp
);



-- 5. Создаем таблицы метаданных:

DROP TABLE IF EXISTS de11tm.ykir_meta_dim_accounts;
CREATE TABLE IF NOT EXISTS de11tm.ykir_meta_dim_accounts(
     max_update_dt
          timestamp
);

DROP TABLE IF EXISTS de11tm.ykir_meta_dim_cards;
CREATE TABLE IF NOT EXISTS de11tm.ykir_meta_dim_cards(
     max_update_dt
          timestamp
);

DROP TABLE IF EXISTS de11tm.ykir_meta_dim_clients;
CREATE TABLE IF NOT EXISTS de11tm.ykir_meta_dim_clients(
     max_update_dt
          timestamp
);

DROP TABLE IF EXISTS de11tm.ykir_meta_dim_clients;
CREATE TABLE IF NOT EXISTS de11tm.ykir_meta_dim_clients(
     max_update_dt
          timestamp
);

DROP TABLE IF EXISTS de11tm.ykir_meta_dim_terminals;
CREATE TABLE IF NOT EXISTS de11tm.ykir_meta_dim_terminals(
     max_update_dt
          timestamp
);

DROP TABLE IF EXISTS de11tm.ykir_meta_fact_passport_blacklist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_meta_fact_passport_blacklist(
     max_update_dt
          timestamp
);

DROP TABLE IF EXISTS de11tm.ykir_meta_fact_transactions;
CREATE TABLE IF NOT EXISTS de11tm.ykir_meta_fact_transactions(
     max_update_dt
          timestamp
);




-- 6. Создаём таблицу отчёта:

DROP TABLE IF EXISTS de11tm.ykir_rep_fraud;
CREATE TABLE IF NOT EXISTS de11tm.ykir_rep_fraud(
     event_dt
          timestamp
,    passport
          varchar(15)
,    fio
          varchar(60)
,    phone
          varchar(16)
,    event_type
          varchar(1)
,    report_dt
          date
);


