# PySpark Data Migration and Transformation with Medallion Architecture


# Overview
This project implements a PySpark data migration and transformation pipeline using the Medallion Architecture (Bronze, Silver, and Gold layers) with Delta Lake. The objective is to extract, transform, and load (ETL) order-related data from MongoDB into Delta Lake and create analytics-ready datasets.

# Project Structure
MongoDB Source: Data is initially extracted from MongoDB collections.
Medallion Architecture Layers:
Bronze Layer: Raw data ingestion.
Silver Layer: Data cleansing, normalization, and enrichment.
Gold Layer: Analytics-ready tables with business-level KPIs and aggregates.
KPIs Computed: Sales metrics and order analytics metrics.

# Implementation Details
1. Environment Setup
  	1. Language: Python 3.7+
  	2. Framework: PySpark (version 3.x)
   3. Libraries:
      1.	delta-spark for Delta Lake support.
2. MongoDB Connection Setup
Initially, the provided MongoDB URI was inaccessible. Therefore, I created a MongoDB Atlas account with a free tier and replicated the data using the schema and sample records as provided in the project documentation.
	1.	Database Name: company_db
	2.	Collections: orders, customers
4. Medallion Architecture Implementation
The pipeline follows the Medallion Architecture’s structured layers to improve data quality and performance:

	1.	Bronze Layer: Raw Data Ingestion
			Extracted raw data from the MongoDB collections (orders, customers) and ingested it into Delta Lake tables bronze_orders and bronze_customers.
			The raw data structure is preserved in this layer to maintain fidelity with the source.
	2.	Silver Layer: Data Cleansing and Enrichment
		Data Cleaning:
			Converted string dates (order_date, shipping_date, registration_date) to date format.
			Removed duplicates and handled null values.
	Data Enrichment:
			Flattened nested structures, particularly the items array in orders, and derived the item_count field.
			Joined bronze_orders and bronze_customers on customer_id to enrich order data with customer details.
			Derived fields such as customer_name (from first_name and last_name) and region (from address).
			Stored the enriched data in Delta Lake silver_orders and silver_customers.
    3.	Gold Layer: Analytics Transformation
			Two analytics tables were created to meet reporting requirements:

Orders Analytics Table:
Derived metrics: order_id, customer_id, order_date, shipping_date, status, total_amount, item_count, customer_name, customer_email, and region.
Cleaned and enriched data from the Silver layer formed the foundation for creating business-level insights.
Sales Metrics Table:
Aggregated data on a regional level with metrics including total_sales, total_orders, average_order_value, first_order_date, and last_order_date.
Enabled insights into order volume, customer spending, and geographical sales trends.
4. KPI Calculation
Based on the requirements, the following KPIs were derived:

Total Sales: Sum of total_amount for all orders.
Total Orders: Unique count of order_id.
Average Order Value: Average of total_amount per order.
Total Quantity Sold: Total quantity of items across orders.
Average Items per Order: Average item count per order.
Order Fulfillment Rate: Percentage of orders marked as Shipped or Completed.
Average Shipping Time: Average days taken to ship orders.
Sales by Product: Total revenue grouped by product_id.
Sales by Customer: Revenue per customer.
Sales Trends: Monthly and yearly breakdown of sales.
5. Code Execution
To execute the pipeline, run the mongodb_to_delta_lake.py script. This script performs the following:

Connects to MongoDB Atlas to retrieve data.
Processes data across the Bronze, Silver, and Gold layers.
Calculates the required KPIs and saves the results in Delta tables for easy querying and reporting.
6. Data Storage
The final Delta Lake tables are stored in the specified directory (/path/to/gold/layer) as:

gold_orders_analytics
gold_sales_metrics
7. Additional Considerations
Z-Ordering: Applied on frequently queried columns to optimize data retrieval in Delta Lake.
Incremental Loads (Optional): Designed for potential extension to handle incremental data loads if data sources are frequently updated.
Example Usage
Run Pipeline
bash
Copy code
spark-submit mongodb_to_delta_lake.py
Dependencies
Ensure the following packages are installed:

bash
Copy code
pip install pymongo delta-spark
Conclusion
This solution provides a robust data migration and transformation framework using PySpark and Delta Lake. The Medallion Architecture ensures data quality and performance, while the analytics tables and KPIs offer valuable business insights.
