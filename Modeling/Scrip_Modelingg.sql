CREATE TABLE "DimCustomer" (
  "customer_id" integer PRIMARY KEY,
  "first_name" varchar,
  "last_name" varchar,
  "address" varchar,
  "gender" varchar,
  "zip_code" varchar
);

CREATE TABLE "DimProduct" (
  "product_id" integer PRIMARY KEY,
  "name" varchar,
  "price" float,
  "category_id" integer,
  "supplier_id" integer
);

CREATE TABLE "DimSupplier" (
  "supplier_id" integer PRIMARY KEY,
  "name" varchar,
  "country" varchar
);

CREATE TABLE "DimCategory" (
  "category_id" integer PRIMARY KEY,
  "name" varchar
);

CREATE TABLE "DimCoupon" (
  "coupon_id" integer PRIMARY KEY,
  "discount_percent" float
);

CREATE TABLE "DimDate" (
  "date_id" integer PRIMARY KEY,
  "date" date,
  "day" integer,
  "month" integer,
  "year" integer,
  "weekday" integer
);

CREATE TABLE "FactSales" (
  "order_id" integer,
  "customer_id" integer,
  "product_id" integer,
  "coupon_id" integer,
  "date_id" integer,
  "amount" integer,
  "total_price" float
);

CREATE TABLE "DimLoginAttempts" (
  "login_attempt_id" integer PRIMARY KEY,
  "customer_id" integer,
  "login_successful" boolean,
  "attempted_at" timestamp
);

CREATE TABLE "FactCustomerActivity" (
  "activity_id" integer PRIMARY KEY,
  "customer_id" integer,
  "order_id" integer,
  "login_attempt_id" integer,
  "activity_date" date,
  "activity_type" varchar
);

CREATE TABLE "DimOrders" (
  "order_id" integer PRIMARY KEY,
  "customer_id" varchar,
  "status" text,
  "created_at" timestamp
);

CREATE TABLE "DimOrder_items" (
  "order_items_id" integer PRIMARY KEY,
  "order_id" integer,
  "product_id" integer,
  "amount" integer,
  "coupon_id" integer
);

ALTER TABLE "FactSales" ADD FOREIGN KEY ("customer_id") REFERENCES "DimCustomer" ("customer_id");

ALTER TABLE "FactSales" ADD FOREIGN KEY ("product_id") REFERENCES "DimProduct" ("product_id");

ALTER TABLE "FactSales" ADD FOREIGN KEY ("coupon_id") REFERENCES "DimCoupon" ("coupon_id");

ALTER TABLE "FactSales" ADD FOREIGN KEY ("date_id") REFERENCES "DimDate" ("date_id");

ALTER TABLE "FactSales" ADD FOREIGN KEY ("order_id") REFERENCES "DimOrders" ("order_id");

ALTER TABLE "DimLoginAttempts" ADD FOREIGN KEY ("customer_id") REFERENCES "DimCustomer" ("customer_id");

ALTER TABLE "DimCategory" ADD FOREIGN KEY ("category_id") REFERENCES "DimProduct" ("category_id");

ALTER TABLE "DimProduct" ADD FOREIGN KEY ("supplier_id") REFERENCES "DimSupplier" ("supplier_id");

ALTER TABLE "FactCustomerActivity" ADD FOREIGN KEY ("customer_id") REFERENCES "DimCustomer" ("customer_id");

ALTER TABLE "FactCustomerActivity" ADD FOREIGN KEY ("login_attempt_id") REFERENCES "DimLoginAttempts" ("login_attempt_id");

ALTER TABLE "FactCustomerActivity" ADD FOREIGN KEY ("activity_date") REFERENCES "DimDate" ("date_id");

ALTER TABLE "FactCustomerActivity" ADD FOREIGN KEY ("order_id") REFERENCES "DimOrders" ("order_id");

ALTER TABLE "DimCustomer" ADD FOREIGN KEY ("customer_id") REFERENCES "DimOrders" ("customer_id");

ALTER TABLE "DimOrder_items" ADD FOREIGN KEY ("order_id") REFERENCES "DimOrders" ("order_id");

ALTER TABLE "DimOrder_items" ADD FOREIGN KEY ("product_id") REFERENCES "DimProduct" ("product_id");

ALTER TABLE "DimOrder_items" ADD FOREIGN KEY ("coupon_id") REFERENCES "DimCoupon" ("coupon_id");
