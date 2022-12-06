/*
1. Выведите список сотрудников (фамилия, имя сотрудника)
и их начальников (фамилия, имя) (HR.EMPLOYEES)
*/

SELECT concat(e.last_name, ', ', e.first_name) AS employee,
	   concat(mg.last_name, ', ', mg.first_name) AS manager
  FROM hr.employees e
 	   JOIN hr.employees mg ON mg.employee_id = e.manager_id;

 	  
 	  

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

--EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) -- для использования на сайте https://tatiyants.com/pev/
SELECT d.email,
	   CASE
	   		WHEN d.email NOT LIKE '%@%' THEN 'некорректная почта' -- отсутствует @
	  		WHEN d.email ~* '\s' THEN 'некорректная почта' -- присутствуют пробелы
	  		WHEN d.email ~* '(\-{2,})|(\.{2,})' THEN 'некорректная почта' -- повторяющиеся .. или --
	   		WHEN d.email !~* '[\w!#$%&*+/=?^_`{|}~.-]@[\w-]'  THEN 'некорректная почта' -- недопустимые символы
	   		ELSE d.email
	   END AS new_email
  FROM de.datasource d;
  	   

  	 
  	  
/*
3. Создайте представление de11tm.XXX_DATASOURCE, в котором необходимо сформировать поля:
a. корректно заполненные first_name, last_name;
b. очищенная почта в email
c. телефон phone_num, отформатированный по маске +7 (123) 456-78-90;
d. унифицированный пол gender.
В качестве источника используется объект de.DATASOURCE
*/
 	
 
-- CREATE OR REPLACE VIEW de11tm.ykir_datasource AS (
  WITH phone AS (
       
 	SELECT d.id,
 		   d.first_name,
 		   d.last_name,
 		   CASE
	   			WHEN d.email NOT LIKE '%@%' THEN 'некорректная почта' -- отсутствует @
	  			WHEN d.email ~* '\s' THEN 'некорректная почта' -- присутствуют пробелы
	  			WHEN d.email ~* '(\-{2,})|(\.{2,})' THEN 'некорректная почта' -- повторяющиеся .. или --
	   			WHEN d.email !~* '[\w!#$%&*+/=?^_`{|}~.]@[\w-]'  THEN 'некорректная почта' -- недопустимые символы
	   	   		ELSE d.email
	   	   END AS email,
	   	   CASE 
	   	   		WHEN d.email ~ '\+?[78](?:[\s()-]*\d){10,}'
	   	               THEN 
	   	                    concat('+7 (',
	   	                    substring(regexp_replace(
	   	                         regexp_match(d.email, '\+?[78](?:[\s()-]*\d){10,}')::TEXT, '\D+', '', 'g'),
	   	                         2, 3), ') ',
	   	                    substring(regexp_replace(
                                   regexp_match(d.email, '\+?[78](?:[\s()-]*\d){10,}')::TEXT, '\D+', '', 'g'),     
	   	                         5, 3), '-',
	   	                    substring(regexp_replace(
                                   regexp_match(d.email, '\+?[78](?:[\s()-]*\d){10,}')::TEXT, '\D+', '', 'g'),
                                   8, 2), '-',
                              substring(regexp_replace(
                                   regexp_match(d.email, '\+?[78](?:[\s()-]*\d){10,}')::TEXT, '\D+', '', 'g'),
                                   10, 2)
                                   )
	   	   END AS phone_num,
	   	   CASE WHEN d.gender = 'F' THEN 'Female'
	   	   		WHEN d.gender = 'M' THEN 'Male'
	   	   		ELSE d.gender 
	   	   END
   	  FROM de.datasource d;
 


	 