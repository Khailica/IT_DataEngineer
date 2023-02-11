/*
1. Выведите список сотрудников (фамилия, имя сотрудника)
и их начальников (фамилия, имя) (HR.EMPLOYEES)
*/

SELECT
     concat(e.last_name, ', ', e.first_name) employee
,    mg.last_name AS last_name_manager
,    mg.first_name AS first_name_manager
FROM
     hr.employees e
LEFT JOIN
     hr.employees mg
          ON mg.employee_id = e.manager_id;  

     
     
/*
2. Создайте запрос, который позволяет найти корректную почту из DE.DATASOURCE
(в скрипте в комментариях необходимо указать что Вы считаете корректной электронной почтой),
если почта некорректная, то укажите 'некорректная почта'
[задача с использованием регулярных выражений]

Локальная часть может включать буквы и цифры в верхнем или нижнем регистре.
Она также может включать ряд специальных символов, включая !#$%&'*+-/=?^_`{|}~, а также точку.
Однако точку нельзя использовать последовательно (две точки вместе).

Доменная часть может также включать заглавные и строчные буквы и цифры, а также дефис.
Однако дефис нельзя использовать и последовательно (два из них вместе)
*/
  	   
SELECT
     d.email
,    CASE
          WHEN d.email NOT LIKE '%@%' THEN -- отсутствует @
               'некорректная почта'
          WHEN d.email ~* '\s' THEN -- присутствуют пробелы
               'некорректная почта'
          WHEN d.email ~* '(\-{2,})|(\.{2,})' THEN -- повторяющиеся .. или --
               'некорректная почта'
          WHEN d.email !~* '[\w!#$%&*+/=?^_`{|}~.-]@[\w-]' THEN -- недопустимые символы
               'некорректная почта'
          ELSE
               d.email
     END new_email
FROM
     de.datasource d;
 	 
  	  

/*
3. Создайте представление de11tm.XXX_DATASOURCE, в котором необходимо сформировать поля:
a. корректно заполненные first_name, last_name;
b. очищенная почта в email
c. телефон phone_num, отформатированный по маске +7 (123) 456-78-90;
d. унифицированный пол gender.
В качестве источника используется объект de.DATASOURCE
*/
 	
 
--CREATE OR REPLACE VIEW de11tm.ykir_datasource AS
     SELECT
          d.id
     ,    CASE
               WHEN d.first_name IS NULL THEN
                    split_part(d.last_name, ' ', 1)
               WHEN trim(d.first_name)~ '\s' THEN
                    split_part(d.first_name, ' ', 1)
               ELSE
                    d.first_name
          END
     ,    CASE
               WHEN d.last_name IS NULL THEN
                    split_part(d.first_name, ' ', 2)
               WHEN trim(d.last_name) ~ '\s' THEN
                    split_part(d.last_name, ' ', 2)
               ELSE
                    d.last_name
          END
     ,    CASE
               WHEN d.email NOT LIKE '%@%' THEN -- отсутствует @
                    'некорректная почта' 
               WHEN d.email ~* '\s' THEN -- присутствуют пробелы
                    'некорректная почта'
               WHEN d.email ~* '(\-{2,})|(\.{2,})' THEN -- повторяющиеся .. или --
                    'некорректная почта'
               WHEN d.email !~* '[\w!#$%&*+/=?^_`{|}~.]@[\w-]' THEN -- недопустимые символы
                    'некорректная почта'
               ELSE
                    d.email
          END email
     ,    CASE
               WHEN d.email ~ '\+?[123456789](?:[\s()-]*\d){10,}' THEN  -- отделяю телефонный номер
                    -- собираю заново по шаблону
                    concat( 
                         '+7 ('
                    ,    substring(
                              regexp_replace(regexp_match(d.email, '\+?[123456789](?:[\s()-]*\d){10,}')::text, '\D+', '', 'g')
                         ,    2
                         ,    3
                         )
                    ,    ') '
                    ,    substring(
                              regexp_replace(regexp_match(d.email, '\+?[123456789](?:[\s()-]*\d){10,}')::text, '\D+', '', 'g')
                         ,    5
                         ,    3
                         )
                    ,    '-'
                    ,    substring(
                              regexp_replace(regexp_match(d.email, '\+?[123456789](?:[\s()-]*\d){10,}')::text, '\D+', '', 'g')
                         ,    8
                         ,    2
                         )
                    ,    '-'
                    ,    substring(
                              regexp_replace(regexp_match(d.email, '\+?[123456789](?:[\s()-]*\d){10,}')::text, '\D+', '', 'g')
                         ,    10
                         ,    2
                         )
                    )
          END phone_num
     ,    CASE
               WHEN d.gender = 'F' THEN
                    'Female'
               WHEN d.gender = 'M' THEN
                    'Male'
               ELSE
                    d.gender
          END
     FROM
          de.datasource d;

 
 
 -- смотрю итог
TABLE de11tm.ykir_datasource;


-- подчищаю за собой   
DROP VIEW IF EXISTS de11tm.ykir_datasource;



 


	 