
/*
3. На основе таблиц CLASS11_credit_payments (платежи по кредиту) подготовьте таблицу в SCD2 версионности,
где будет отражена общая сумма выплат клиента Банку на дату по каждому договору
*/



SELECT
     deal_id
,    sum(payment_sum_amt) OVER(PARTITION BY deal_id ORDER BY payment_dt) AS total_payment_sum_amt
,    payment_dt
,    coalesce(
          lead(payment_dt) OVER(PARTITION BY deal_id ORDER BY payment_dt) - '1 second'::interval
     ,    '5999-12-31 23:59:59'
     ) AS payment_dt_end
,    delq_flg
FROM
     de11tm.class11_credit_payments
ORDER BY
     deal_id
,    payment_dt;