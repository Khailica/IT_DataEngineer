/*
Групповой проект
В рамках группового проекта необходимо сформировать витрину с информацией по клиенту,
все показатели должны быть рассчитаны на 1 декабря 2021 года.


Показатели формируются по следующим блокам данных:
* Соц.-демо;
* Кредитные заявки;
* Данные Бюро Кредитных Историй (далее – БКИ);
* Карточные транзакции;
* Зарплатные транзакции.


Ниже приведен перечень показателей, которые необходимо сформировать по клиенту:        
1. Блок соц.-демо: 
* Дата рождения; 
* Возраст (целое число лет);
* Код региона;
* Адрес регистрации; 
* Адрес проживания; 
* Пол (наименование);
* Уровень образования (наименование);
* Семейное положение (наименование); 
* Рабочий стаж (целое кол-во лет); 
* Доля жизни клиента, которую он работал;
* Является ли сотрудником Банка;
* Кол-во лет с последнего изменения имени (если не заполнено, то оставить NULL);


Примечание: адреса регистрации и проживания должны содержать Наименование и тип региона,
при наличии наименование и тип района, при наличии тип и наименование города/населенного пункта. 

Для всех сформированных показателей: если нет информации и поле текстовое, то необходимо указать 'Нет данных',
если поле числовое, то 0, при условии, что иного не сказано в описании поля.
Дробные числа округляются до 2 знаков после запятой, если иного не сказано в описании поля.
Наименование полей в финальной витрине должно соответствовать наименованию полей в файле ‘Групповой_проект_Наименование_полей.xlsx’
*/

TABLE de11tm.group_dim_client;
TABLE de11tm.group_dim_client_address ORDER BY client_id;
TABLE de11tm.group_dict_gender;
TABLE de11tm.group_dict_education_level;
TABLE de11tm.group_dict_family_status;




-- адреса 
WITH client_concat_address AS (
     SELECT
          dca.client_id
     ,    dca.addr_type
     ,    CASE
               WHEN dca.region = dca.city THEN
                    concat(
                         'Город '
                    ,    dca.city)
               ELSE
                    concat(
                         dca.region, ' '
                    ,    COALESCE(dca.region_type, 'Область'), ', '
                    ,    CASE
                              WHEN dca.district IS NOT NULL THEN 
                                   concat(dca.district, ' район')
                         END
                    ,    CASE
                              WHEN dca.city IS NOT NULL THEN
                                   'город '
                         END
                    ,    dca.city
                    ,    CASE
                              WHEN dca.town IS NOT NULL THEN
                                   concat(', ', dca.town_type, ' ', dca.town)
                         END
                         )
          END AS "Адрес"
     FROM
         de11tm.group_dim_client_address dca
)
SELECT
     dc.client_id
,    to_date(NULLIF(dc.birth_dt, 'NULL'), 'DDMONYYYY') AS "Дата рождения"
,    EXTRACT(YEAR FROM age('2021-12-01', to_date(NULLIF(dc.birth_dt, 'NULL'), 'DDMONYYYY'))) AS "Возраст"
,    dc.region_code AS "Код региона"
,    cca1."Адрес" AS "Адрес регистрации"
,    cca2."Адрес" AS "Адрес проживания"
,    COALESCE(dg.gender_nm, 'Нет данных') AS "Пол"
,    COALESCE(del.level_nm, 'Нет данных') AS "Уровень образования"
,    COALESCE(dfs.status_nm, 'Нет данных') AS "Семейное положение"
,    dc.fullseniority_year_cnt AS "Рабочий стаж"
--,    AS "Доля жизни клиента, которую он работал"
,    dc.staff_flg AS "Является ли сотрудником Банка"
,    EXTRACT(YEAR FROM age('2021-12-01', to_date(CAST(dc.name_change_year AS TEXT),  'YYYY'))) AS "Кол-во лет изменения имени"
FROM
  de11tm.group_dim_client AS dc
JOIN
  client_concat_address AS cca1
    ON cca1.client_id = dc.client_id AND 
    cca1.addr_type = 1
JOIN
  client_concat_address AS cca2
    ON cca2.client_id = dc.client_id AND 
    cca2.addr_type = 2
LEFT JOIN
  de11tm.group_dict_gender AS dg
    ON dg.gender_code = dc.gender_code
LEFT JOIN
  de11tm.group_dict_education_level AS del
    ON del.level_code = dc.education_level_code
LEFT JOIN
  de11tm.group_dict_family_status AS dfs
    ON dfs.status_code = dc.family_status_code
ORDER BY
     dc.client_id;
     




	 