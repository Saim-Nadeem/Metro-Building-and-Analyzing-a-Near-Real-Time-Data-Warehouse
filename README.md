# Metro-Building-and-Analyzing-a-Near-Real-Time-Data-Warehouse

This project involves building and analyzing a near-real-time Data Warehouse for METRO Shopping Store. It includes setting up a star schema for the data warehouse, implementing the MESHJOIN algorithm in Java for processing transactional data, and performing OLAP analysis using SQL queries.

## Prerequisites

Before you begin, ensure you have the following tools installed:

1. **MySQL Workbench**  
   Ensure MySQL is installed and available via the MySQL Workbench tool.

2. **Eclipse IDE**  
   Download and install **Eclipse IDE** along with a JDK (Java Development Kit) setup.

3. **MySQL Connector**  
   Add the **MySQL Connector** to your Eclipse project for establishing database connections.

4. **CSV Files**  
   Ensure you have the following CSV files:
   - `customers_data.csv`
   - `products_data.csv`
   - `transaction.csv`

---

## Setup and Run Instructions

Follow these steps to set up and run the project:

### Step 1: MySQL Configuration
  - Open **MySQL Workbench** and run the following query to get the file path location:
  ```sql
  SELECT @@secure_file_priv;
  ```
  - The path received is were you have to place the product and customer csv.

### Step 2: Modify and run `Meta_Data.sql`

- Open the **Meta_Data.sql** file in your project.
- Update the CSV file path in the script according to the path received in **Step 1**.
- Save the file and run it in **MySQL Workbench**.

### Step 3: Create Data Warehouse Schema

- Open the **data_warehouse.sql** file in **MySQL Workbench**.
- Run the script to create the schema for the data warehouse.

### Step 4: Eclipse Project Setup

- Open the **Eclipse project**.
- Place the `transaction.csv` file in the src folder of the Eclipse project.
- Ensure the **MySQL connector** is added to the Eclipse project.
- Run the Java project to load transactional data into the data warehouse.

### Step 5: Execute OLAP Queries

- Open the **sql_query.sql** file in **MySQL Workbench**.
- Run the queries one by one to perform the OLAP analysis and retrieve results.

### Step 6: Review the Project Report

- Open and review the **Project Report** to understand the implementation of the MESHJOIN algorithm and OLAP analysis.
