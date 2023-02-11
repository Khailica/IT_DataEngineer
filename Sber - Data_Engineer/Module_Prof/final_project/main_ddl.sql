-- Создаём таблицы измерений в приёмнике:

DROP TABLE IF EXISTS de11tm.ykir_dwh_dim_accounts_hist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_dwh_dim_accounts_hist(
     account_num
          varchar(20)
,    valid_to
          date
,    client
          varchar(10)
,    effective_from
          timestamp(0)
,    effective_to
          timestamp(0)
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
          timestamp(0)
,    effective_to
          timestamp(0)
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
          timestamp(0)
,    effective_to
          timestamp(0)
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
          timestamp(0)
,    effective_to
          timestamp(0)
               DEFAULT '5999-12-31 00:00:00'
,    deleted_flg
          smallint
               DEFAULT 0
);



-- Создаём таблицы фактов в приёмнике:

DROP TABLE IF EXISTS de11tm.ykir_dwh_fact_passport_blacklist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_dwh_fact_passport_blacklist(
     passport_num
          varchar(15)
,    entry_dt
          date
);



DROP TABLE IF EXISTS de11tm.ykir_dwh_fact_transactions;
CREATE TABLE IF NOT EXISTS de11tm.ykir_dwh_fact_transactions(
     trans_id varchar(20)
,    trans_date timestamp(0)
,    card_num varchar(20)
,    oper_type varchar(7)
,    amt numeric(10,2)
,    oper_result varchar(10)
,    terminal varchar(10)
);


     account_num
          varchar(20)
,    valid_to
          date
,    client
          varchar(10)
,    effective_from
          timestamp(0)

          
          
-- Создаём стейджинговые таблицы:
          
DROP TABLE IF EXISTS de11tm.ykir_stg_dim_accounts;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_dim_accounts(
     account_num
          varchar(20)
,    valid_to
          date
,    client
          varchar(10)
,    create_dt
          timestamp(0)
,    update_dt
          timestamp(0)
);
  

DROP TABLE IF EXISTS de11tm.ykir_dwh_stg_dim_cards;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_dim_cards(
     card_num
          varchar(20)
,    account_num
          varchar(20)
,    create_dt
          timestamp(0)
,    update_dt
          timestamp(0)
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
          timestamp(0)
,    update_dt
          timestamp(0)
);


DROP TABLE IF EXISTS de11tm.ykir_stg_fact_passport_blacklist;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_fact_passport_blacklist(
     passport_num
          varchar(15)
,    entry_dt
          date
,    create_dt -- возможно лишняя запись
          timestamp(0)
,    update_dt -- возможно лишняя запись
          timestamp(0)
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
,    create_dt -- возможно лишняя запись
          timestamp(0)
,    update_dt
          timestamp(0)
);


DROP TABLE IF EXISTS de11tm.ykir_stg_fact_transactions;
CREATE TABLE IF NOT EXISTS de11tm.ykir_stg_fact_transactions(
     transaction_id varchar(20)
,    transaction_date timestamp(0)
,    amount numeric(10,2) -- возможно тип надо varchar(15)
,    card_num varchar(20)
,    oper_type varchar(7)
,    oper_result varchar(10)
,    terminal varchar(10)
,    create_dt -- возможно лишняя запись
          timestamp(0)
,    update_dt -- возможно лишняя запись
          timestamp(0)
);