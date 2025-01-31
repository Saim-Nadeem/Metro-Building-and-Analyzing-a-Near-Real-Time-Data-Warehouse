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
