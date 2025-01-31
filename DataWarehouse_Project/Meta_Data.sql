-- first removing database if exists and then creating one
drop database if exists meta_data;
create Database meta_data;
use meta_data;

-- creating customer and product table
create table customers(
	customer_id int primary key,
    customer_name varchar(100),
    gender varchar(10)
);

create table products(
	productID int primary key,
    productName varchar(200),
    productPrice decimal(10,2),
    supplierID int,
    supplierName varchar(200),
    storeID int,
    storeName varchar(200)
);

-- creating temporary tables for customer and products to preprocess then insert in main tables
create table temp1(
	customer_id int,
    customer_name varchar(100),
    gender varchar(10)
);

create table temp2(
	productID int primary key,
    productName varchar(200),
    productPrice varchar(100),
    supplierID int,
    supplierName varchar(200),
    storeID int,
    storeName varchar(200)
);

-- checking where to place the files to upload in mysql 
select @@secure_file_priv;

-- loading csv in temp tables
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers_data.csv'
into table temp1 fields terminated by ','
ignore 1 lines;

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products_data.csv'
into table temp2 fields terminated by ','
optionally enclosed by '"'
ignore 1 lines;

-- inserting data from temp tables into customer and product table 
insert ignore into customers
select distinct customer_id,customer_name,gender from temp1; 

insert  into products
select productID,productName,CONVERT(REPLACE(productPrice,'$',''),DECIMAL(10,2)),supplierID,supplierName,storeID,storeName from temp2;

-- dropping tempory tables
drop table temp1;
drop table temp2;