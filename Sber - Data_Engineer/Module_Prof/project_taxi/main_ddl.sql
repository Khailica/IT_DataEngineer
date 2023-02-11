CREATE TABLE IF NOT EXISTS dwh_novocherkassk.dim_drivers(
     personnel_num
          serial
,    last_name
          varchar(20)
,    first_name
          varchar(20)
,    middle_name
          varchar(20)
,    birth_dt
          date
,    card_num
          varchar(19)
,    driver_license_num
          varchar(12)
,    driver_license_dt
          date
,    create_dttm
          timestamp(0)
,    update_dttm
          timestamp(0)
,    processed_dt
          timestamp(0)
);


CREATE TABLE IF NOT EXISTS dwh_novocherkassk.fact_rides(
     ride_id
          integer
,    point_from_txt
          varchar(200)
,    point_to_txt
          varchar(200)
,    distance_val
          numeric(5,2)
,    price_amt
          numeric(7,2)
,    client_phone_num
          varchar(18)
,    driver_pers_num
          integer
,    car_plate_num
          varchar(9)
,    ride_arrival_dt
          timestamp(0)
,    ride_start_dt
          timestamp(0)
,    ride_end_dt
          timestamp(0)
,    processed_dt
          timestamp(0)
);


CREATE TABLE IF NOT EXISTS dwh_novocherkassk.fact_waybills(
     waybill_num
          varchar(6)
,    driver_pers_num
          integer
,    car_plate_num
          varchar(9)
,    work_start_dt
          timestamp(0)
,    work_end_dt
          timestamp(0)
,    issue_dt
          timestamp(0)
,    processed_dt
          timestamp(0)
);



CREATE TABLE IF NOT EXISTS dwh_novocherkassk.stg_fact_rides(
     ride_id
          integer
,    point_from_txt
          varchar(200)
,    point_to_txt
          varchar(200)
,    distance_val
          numeric(5,2)
,    price_amt
          numeric(7,2)
,    client_phone_num
          varchar(18)
,    driver_pers_num
          integer
,    car_plate_num
          varchar(9)
,    ride_arrival_dt
          timestamp(0)
,    ride_start_dt
          timestamp(0)
,    ride_end_dt
          timestamp(0)
);



CREATE TABLE IF NOT EXISTS dwh_novocherkassk.stg_rides(
     ride_id
          integer
,    dt
          timestamp(0)
,    client_phone
          varchar(18)
,    card_num
          varchar(19)
,    point_from
          varchar(200)
,    point_to
          varchar(200)
,    distance
          numeric(5,2)
,    price
          numeric(7,2)
);


CREATE TABLE IF NOT EXISTS dwh_novocherkassk.stg_movement(
     movement_id
          integer
,    car_plate_num
          varchar(9)
,    ride
          integer
,    "event"
          varchar(6)
,    dt
          timestamp(0)
);


CREATE TABLE IF NOT EXISTS dwh_novocherkassk.stg_drivers(
     driver_license
          varchar(12)
,    first_name
          varchar(20)
,    last_name
          varchar(20)
,    middle_name
          varchar(20)
,    driver_valid_to
          date
,    card_num
          varchar(19)
,    update_dt
          timestamp(0)
,    birth_dt
          date
);


CREATE TABLE IF NOT EXISTS dwh_novocherkassk.stg_waybills(
     waybill_num
          varchar(6)
,    car_plate_num
          varchar(9)
,    work_start_dt
          timestamp(0)
,    work_end_dt
          timestamp(0)
,    issue_dt
          timestamp(0)
,    license
          varchar(12)
);


CREATE TABLE IF NOT EXISTS dwh_novocherkassk.rep_drivers_payments(
     personnel_num
          integer
,    last_name
          varchar(20)
,    first_name
          varchar(20)
,    middle_name
          varchar(20)
,    card_num
          varchar(19)
,    amount
          numeric(10,2)
,    report_dt
          date
);