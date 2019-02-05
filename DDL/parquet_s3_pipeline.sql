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



drop pipeline if exists parquet_orders;
create pipeline parquet_orders as
load data s3 'mdemarco-s3/parquet_files/orders.parquet'
CREDENTIALS 
'{"aws_access_key_id": "<add your key>", 
"aws_secret_access_key": "<add your secret"}'
with transform ('file:///tmp/pq_reader.py','','')
IGNORE
INTO table orders
	FIELDS TERMINATED BY ','
	OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    (order_id,order_date,order_customer_id,order_status);
  