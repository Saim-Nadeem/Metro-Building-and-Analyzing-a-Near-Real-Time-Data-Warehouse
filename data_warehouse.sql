-- first removing database if exists and then creating one
drop database if exists data_warehouse;
create Database data_warehouse;
use data_warehouse;

-- creating customer table
create table customers(
	customer_id int primary key,
    customer_name varchar(100),
    gender varchar(10)
);

-- creating product table
create table products(
	productID int primary key,
    productName varchar(200),
    productPrice decimal(10,2)
); 

-- creating store table
create table store(
	storeID int primary key,
    storeName varchar(200)
); 

-- creating suplier table
create table supplier(
	supplierID int primary key,
    supplierName varchar(200)
); 

-- creating date table
create table time(
	time_id int primary key,
    time_date date,
    time_day int,
    time_month int,
    time_quarter varchar(3),
    time_year int(11),
    weekend int,
    half_year int
    );

-- creating fact table sales
create table sales(
	transition_id int primary key,
    total_sales double,
    quantity double,
    customer_id int,
    productID int,
    storeID int,
    supplierID int,
    time_id int,
    Order_ID int,
    foreign key (customer_id) references customers(customer_id), 
    foreign key (productID) references products(productID),
    foreign key (storeID) references store(storeID), 
    foreign key (supplierID) references supplier(supplierID), 
    foreign key (time_id) references time(time_id)
);