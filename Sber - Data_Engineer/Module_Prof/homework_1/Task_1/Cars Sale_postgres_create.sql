--https://dbdesigner.page.link/o9vPt5GN3nXZEUrM7

CREATE TABLE de11tm.ykir_Car (
	"car_id" serial NOT NULL,
	"model_id" int NOT NULL,
	"production_date" DATE NOT NULL,
	CONSTRAINT "Car_pk" PRIMARY KEY ("car_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE de11tm.ykir_Car_model (
	"model_id" serial NOT NULL,
	"specifications" TEXT NOT NULL,
	CONSTRAINT "Car_model_pk" PRIMARY KEY ("model_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE de11tm.ykir_Client (
	"client_id" serial NOT NULL,
	"first_name" varchar(20) NOT NULL,
	"last_name" varchar(20) NOT NULL,
	"phone" varchar(20) NOT NULL,
	CONSTRAINT "Client_pk" PRIMARY KEY ("client_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE de11tm.ykir_Sales (
	"sale_id" serial NOT NULL,
	"car_id" integer NOT NULL,
	"client_id" integer NOT NULL,
	"manager_id" integer NOT NULL,
	"sale_date" DATE NOT NULL,
	"total_price" real NOT NULL,
	"quantity" integer NOT NULL,
	CONSTRAINT "Sales_pk" PRIMARY KEY ("sale_id")
) WITH (
  OIDS=FALSE
);



CREATE TABLE de11tm.ykir_Manager (
	"manager_id" serial NOT NULL,
	"first_name" varchar(20) NOT NULL,
	"last_name" varchar(20) NOT NULL,
	"phone" varchar(20) NOT NULL,
	CONSTRAINT "Manager_pk" PRIMARY KEY ("manager_id")
) WITH (
  OIDS=FALSE
);



ALTER TABLE de11tm.ykir_Car ADD CONSTRAINT "Car_fk0" FOREIGN KEY ("model_id") REFERENCES de11tm.ykir_Car_model("model_id");
ALTER TABLE de11tm.ykir_Sales ADD CONSTRAINT "Sales_fk0" FOREIGN KEY ("car_id") REFERENCES de11tm.ykir_Car("car_id");
ALTER TABLE de11tm.ykir_Sales ADD CONSTRAINT "Sales_fk1" FOREIGN KEY ("client_id") REFERENCES de11tm.ykir_Client("client_id");
ALTER TABLE de11tm.ykir_Sales ADD CONSTRAINT "Sales_fk2" FOREIGN KEY ("manager_id") REFERENCES de11tm.ykir_Manager("manager_id");