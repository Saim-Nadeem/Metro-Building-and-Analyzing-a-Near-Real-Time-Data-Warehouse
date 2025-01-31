# Metro-Building-and-Analyzing-a-Near-Real-Time-Data-Warehouse

Prerequisites
1. MySQL Workbench: Validate the installation of MySQL by checking if it's available through the Workbench tool.
2. Eclipse IDE- Download Eclipse, and install a JDK for setup.
3. Connector for MySQL Adding MySQL Connector in an Eclipse Project.
4. CSV files:
   =>customers_data.csv
   =>products_data.csv
   =>transaction.csv


Follow the below steps to setup and run the project:

Step 1:
    =>Open MySQL Workbench and type in the query "SELECT @@secure_file_priv"
    =>The path received is were you have to place the product and customer csv.

Step 2:
    =>Open Meta_Data.sql file and change the csv path in it according to new path for customer and product csvs.
    =>Save and run the sql file.

Step 3:
    =>open data_warehouse.sql file in workbench and run it to create schema of data warehouse.

Step 4:
    =>open Eclipse project.
    =>place the transaction.csv in src folder of eclipse project.
    =>make sure mysql connector is added in eclipse project.
    =>run the project.

Step 5:
    =>open sql_query.sql file on workbench and run queries one by one.

Step 6:
    =>see the report to undetstand what is implemented 
