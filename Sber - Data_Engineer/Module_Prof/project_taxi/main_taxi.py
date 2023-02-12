#!/usr/bin/python3

import psycopg2 as ps
import pandas as pd
import os
import xml.etree.ElementTree as et

conn_src = ps.connect(host = 'de-edu-db.chronosavant.ru',
                        port=  '5432',
                        database= 'taxi',
                        user= 'etl_tech_user',
                        password= 'etl_tech_user_password'
)

conn_tgt = ps.connect(host = 'de-edu-db.chronosavant.ru',
                        port=  '5432',
                        database= 'dwh',
                        user= 'dwh_novocherkassk',
                        password= 'dwh_novocherkassk_QbuWmGhe'
)

conn_src.autocommit = False
conn_tgt.autocommit = False

curs_src = conn_src.cursor()
curs_tgt = conn_tgt.cursor()

#Очищаем stg таблички
curs_tgt.execute("""delete from dwh_novocherkassk.stg_drivers""")
curs_tgt.execute("""delete from dwh_novocherkassk.stg_rides""")
curs_tgt.execute("""delete from dwh_novocherkassk.stg_movement""")
curs_tgt.execute("""delete from dwh_novocherkassk.stg_waybills""")
curs_tgt.execute("""delete from dwh_novocherkassk.stg_fact_rides""")



# 2. Захват данных из источника в STG
#для update и insert

#stg_drivers
curs_src.execute("""SELECT
     driver_license
,    first_name
,    last_name
,    middle_name
,    driver_valid_to
,    card_num
,    update_dt
,    birth_dt
FROM
     main.drivers
LIMIT 20""")
     
res = curs_src.fetchall()

names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns = names)


curs_tgt.executemany("""INSERT INTO dwh_novocherkassk.stg_drivers(
	driver_license
,	first_name
,	last_name
,	middle_name
,	driver_valid_to
,	card_num
,	update_dt
,	birth_dt
)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)""", df.values.tolist())


#stg_rides
curs_src.execute("""SELECT
	ride_id
,	dt
,	client_phone
,	card_num
,	point_from
,	point_to
,	distance
,	price
FROM
	main.rides
LIMIT 20""")
    
res = curs_src.fetchall()

names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns = names)

curs_tgt.executemany("""INSERT INTO dwh_novocherkassk.stg_rides(
	ride_id
,	dt
,	client_phone
,	card_num
,	point_from
,	point_to
,	distance
,	price
)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)""", df.values.tolist())


#stg_movement
curs_src.execute("""SELECT
	movement_id
,	car_plate_num
,	ride
,	event
,	dt
FROM
	main.movement
LIMIT 20""")

res = curs_src.fetchall()

names = [name[0] for name in curs_src.description]
df = pd.DataFrame(res, columns = names)

curs_tgt.executemany("""INSERT INTO dwh_novocherkassk.stg_movement(
	movement_id
,	car_plate_num
,	ride
,	"event"
,	dt
)
VALUES(%s, %s, %s, %s, %s)""", df.values.tolist())


#stg_waybills
df_all = pd.DataFrame(columns = ['waybill_number', 'car_num', 'license', 'start', 'stop', 'issue_dt'])

for file in os.listdir( '/mnt/files/waybills' ):
    tree = et.parse(f'/mnt/files/waybills/{file}')
    root = tree.getroot()
    for waybill in root.findall('waybill'):
        waybill_num = waybill.attrib['number']
        issue_dt = waybill.attrib['issuedt']
        car = waybill.find('car').text
        for driver in waybill.findall('driver'):
            license = driver.find('license').text
        for period in waybill.findall('period'):
            start = period.find('start').text
            stop = period.find('stop').text
    df = pd.DataFrame([[waybill_num, license, car, start, stop, issue_dt]], columns = ['waybill_number', 'car_num', 'license', 'license_validto', 'start', 'stop'])
    df_all = pd.concat([df, df_all])
    
curs_tgt.executemany("""INSERT INTO dwh_novocherkassk.stg_waybills(
	waybill_num
,	driver_pers_num
,	car_plate_num
,	work_start_dt
,	work_end_dt
,	issue_dt
)
VALUES(%s, %s, %s, %s, %s, %s)""", df_all.values.tolist())


# 3. Обновляем обновленные строки в хранилище

#3.1 Работа с dim_drivers - таблица-измерений

#insert новых записей
curs_tgt.execute("""INSERT INTO dwh_novocherkassk.dim_drivers(
     personnel_num
,    last_name
,    first_name
,    middle_name
,    birth_dt
,    card_num
,    driver_license_num
,    driver_license_dt
,    create_dttm
,    update_dttm
,    processed_dt
)
SELECT
     stg.last_name
,    stg.first_name
,    stg.middle_name
,    stg.birth_dt
,    stg.card_num
,    stg.driver_license
,    stg.driver_valid_to
,    stg.update_dt
,    NULL
,    now() processed_dt
FROM
     dwh_novocherkassk.stg_drivers stg
LEFT JOIN
     dwh_novocherkassk.dim_drivers t
          ON stg.driver_license = t.driver_license_num
WHERE
     t.driver_license_num IS NULL""")
	

#update старых записей	
curs_tgt.execute("""UPDATE
	dwh_novocherkassk.dim_drivers tgt
SET
	last_name = tmp.last_name
,	first_name = tmp.first_name
,	middle_name = tmp.middle_name
,	birth_dt = tmp.birth_dt
,	card_num = tmp.card_num
,	driver_license_dt = tmp.driver_license_dt
,	update_dttm = tmp.update_dt
,	processed_dt = now()
FROM
	(
		SELECT
			s.last_name
		,	s.first_name
		,	s.middle_name
		,	s.birth_dt
		,	s.card_num
		,	s.driver_license_dt
		,	s.driver_license
		,	s.update_dt
		FROM
			dwh_novocherkassk.stg_drivers s
		JOIN
			dwh_novocherkassk.dim_drivers t
				ON s.driver_license = t.driver_license_num
	) tmp
WHERE
	tgt.driver_license = tmp.driver_license_num """)



# 4. Удаляем удаленные записи в целевой таблице
curs_tgt.execute("""DELETE FROM
	dwh_novocherkassk.dim_drivers
WHERE
	driver_license_num IN (
		SELECT
			target.driver_license_num
		FROM
			dwh_novocherkassk.dim_drivers tgt
		LEFT JOIN
			dwh_novocherkassk.stg_drivers stg
				ON tgt.driver_license_num = stg.driver_license
		WHERE
			stg.driver_license IS NULL
	) """)



### 3.2 Работа с fact_waybills - таблица-фактов

#insert новых записей
curs_tgt.execute("""INSERT INTO dwh_novocherkassk.fact_waybills(
	waybill_num
,	driver_pers_num
,	car_plate_num
,	work_start_dt
,	work_end_dt
,	issue_dt
,	processed_dt
)
SELECT
	stg.waybill_num
,	stg.driver_pers_num
,	stg.car_plate_num
,	stg.work_start_dt
,	stg.work_end_dt
,	stg.issue_dt
,	now() processed_dt
FROM
	dwh_novocherkassk.stg_waybills stg
LEFT JOIN
	dwh_novocherkassk.fact_waybills t
		ON stg.waybill_num = t.waybill_num
WHERE
	t.waybill_num IS NULL""")
    
    
### 3.3 Работа с fact_rides - таблица-фактов

#insert новых записей
curs_tgt.execute("""INSERT INTO dwh_novocherkassk.stg_fact_rides(
	ride_id
,	point_from_txt
,	point_to_txt
,	distance_val
,	price_amt
,	client_phone_num
,	driver_pers_num
,	car_plate_num
,	ride_arrival_dt
,	ride_start_dt
,	ride_end_dt
)
WITH tmp AS (
	SELECT
		r.ride_id
	,	r.client_phone
	,	r.point_from
	,	r.point_to
	,	r.distance
	,	r.price
	,	max(m.car_plate_num) car_plate_num
	,	max(
			CASE
				WHEN m.event = 'READY' THEN
					m.dt
			END
		) ride_arrival_dt
	,	max(
			CASE
				WHEN m.event = 'BEGIN' THEN
					m.dt
			END
		) ride_start_dt
	,	max(
			CASE
				WHEN m.event IN ('END', 'CANCEL') THEN
					m.dt
			END
		) ride_end_dt
	FROM
		dwh_novocherkassk.stg_rides r
	LEFT JOIN
		dwh_novocherkassk.stg_movement m
			ON r.ride_id = m.ride
	GROUP BY
		r.ride_id
	,	r.client_phone
	,	r.point_from
	,	r.point_to
	,	r.distance
	,	r.price
)
SELECT
	tmp.ride_id
,	tmp.point_from
,	tmp.point_to
,	tmp.distance
,	tmp.price
,	tmp.client_phone
,	w.driver_pers_num
,	tmp.car_plate_num
,	tmp.ride_arrival_dt
,	tmp.ride_start_dt
,	tmp.ride_end_dt
FROM
	tmp
LEFT JOIN
	dwh_novocherkassk.stg_waybills w
		ON tmp.car_plate_num = w.car_plate_num AND
		tmp.ride_arrival_dt BETWEEN w.work_start_dt AND w.work_end_dt
WHERE
	ride_end_dt IS NOT NULL""")
    
    
curs_tgt.execute("""INSERT INTO dwh_novocherkassk.fact_rides(
	ride_id
,	point_from_txt
,	point_to_txt
,	distance_val
,	price_amt
,	client_phone_num
,	driver_pers_num
,	car_plate_num
,	ride_arrival_dt
,	ride_start_dt
,	ride_end_dt
,	processed_dt
)
SELECT
	stg.ride_id
,	stg.point_from
,	stg.point_to
,	stg.distance
,	stg.price
,	stg.client_phone
,	stg.driver_pers_num
,	stg.car_plate_num
,	stg.ride_arrival_dt
,	stg.ride_start_dt
,	stg.ride_end_dt
,	now() processed_dt
FROM
	dwh_novocherkassk.stg_fact_rides stg
LEFT JOIN
	dwh_novocherkassk.fact_rides t
		ON stg.ride_id = t.ride_id
WHERE
	t.ride_id IS NULL""")
    
    
    
### 4. Создание отчета rep_drivers_payments

curs_tgt.execute("""INSERT INTO dwh_novocherkassk.rep_drivers_payments(
	personnel_num
,	last_name
,	first_name
,	middle_name
,	card_num
,	amount
,	report_dt
)
SELECT
	dr.personnel_num
,	dr.last_name
,	dr.first_name
,	dr.middle_name
,	dr.card_num
,	sum(
		rid.price_amt - 0.2 * rid.price_amt - 47.26 * 7 * rid.distance_val / 100 - 5 * rid.distance_val
	) amount
,	(now() - '1'::interval(8))::date report_dt
FROM
	dwh_novocherkassk.dim_drivers dr
LEFT JOIN
	dwh_novocherkassk.fact_rides rid
		ON dr.driver_license_num = rid.driver_pers_num AND
		rid.ride_end_dt::date = now()::date - '1'::interval(8)
GROUP BY
	dr.personnel_num
,	dr.last_name
,	dr.first_name
,	dr.middle_name
,	distance_val
,	dr.card_num""")

conn_tgt.commit()

conn_src.close()
conn_tgt.close()