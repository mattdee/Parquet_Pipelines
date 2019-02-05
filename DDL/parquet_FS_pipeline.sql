create database if not exists parquet;
use parquet;


create table if not exists orders
	(
		order_id			bigint,
		order_date			longtext,
		order_customer_id	longtext,
		order_status		longtext,
		key (order_id,order_date,order_customer_id,order_status)
		using clustered columnstore
		);



drop pipeline if exists fs_parquet_orders;
create pipeline fs_parquet_orders as
load data fs '/tmp/orders/'
with transform ('file:///tmp/pq_reader.py','','')
IGNORE
INTO table orders
	FIELDS TERMINATED BY ','
	OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    (order_id,order_date,order_customer_id,order_status);
  