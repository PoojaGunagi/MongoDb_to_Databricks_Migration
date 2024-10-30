# PySpark Data Migration and Transformation with Medallion Architecture


# Overview
This project implements a PySpark data migration and transformation pipeline using the Medallion Architecture (Bronze, Silver, and Gold layers) with Delta Lake. The objective is to extract, transform, and load (ETL) order-related data from MongoDB into Delta Lake and create analytics-ready datasets.

# Project Structure
1. MongoDB Source:
	Data is initially extracted from MongoDB collections.
4. Medallion Architecture Layers:
	1. Bronze Layer: Raw data ingestion.
	2. Silver Layer: Data cleansing, normalization, and enrichment.
	3. Gold Layer: Analytics-ready tables with business-level KPIs and aggregates.
5. KPIs Computed: 
	Sales metrics and order analytics metrics.

# Implementation Details
1. Environment Setup
  	1. Language: Python 3.7+
  	2. Framework: PySpark (version 3.x)
   3. Libraries:
      1.	delta-spark for Delta Lake support.
2. MongoDB Connection Setup
Initially,The provided MongoDB URI was inaccessible. Therefore, I created a MongoDB Atlas account with a free tier and replicated the data using the schema and sample records as provided in the project documentation.
	1.	Database Name: Sample
	2.	Collections: Orders, Customers
	 3.	ConnectionString: 'mongodb+srv://poojaggunagi19:rthlXogUzrxSIuiK@cluster0.e6d2i.mongodb.net/'
4. Medallion Architecture Implementation
The pipeline follows the Medallion Architecture’s structured layers to improve data quality and performance:

	1.	Bronze Layer: Raw Data Ingestion
			Extracted raw data from the MongoDB collections (orders, customers) and ingested it into Delta Lake tables bronze_orders and bronze_customers.
			The raw data structure is preserved in this layer to maintain fidelity with the source.
	2.	Silver Layer: Data Cleansing and Enrichment
		1. Data Cleaning:
     			Converted string dates (order_date, shipping_date, registration_date) to date format.
     			Removed duplicates and handled null values.
     		2.	Data Enrichment:
			Flattened nested structures, particularly the items array in orders, and derived the item_count field.
			Joined bronze_orders and bronze_customers on customer_id to enrich order data with customer details.
			Derived fields such as customer_name (from first_name and last_name) and region (from address).
			Stored the enriched data in Delta Lake silver_orders and silver_customers.
    3.	Gold Layer: Analytics Transformation
			Two analytics tables were created to meet reporting requirements:

		1. Orders Analytics Table:
			Derived metrics: order_id, customer_id, order_date, shipping_date, status, total_amount, item_count, customer_name, customer_email, and region.
			Cleaned and enriched data from the Silver layer formed the foundation for creating business-level insights.
		2. Sales Metrics Table:
			Aggregated data on a regional level with metrics including total_sales, total_orders, average_order_value, first_order_date, and last_order_date.
			Enabled insights into order volume, customer spending, and geographical sales trends.
4. KPI Calculation

	Based on the requirements, the following KPIs were derived:

	1. Total Sales: Sum of total_amount for all orders.
	2. Total Orders: Unique count of order_id.
	3. Average Order Value: Average of total_amount per order.
	4. Total Quantity Sold: Total quantity of items across orders.
	5. Average Items per Order: Average item count per order.
	6. Order Fulfillment Rate: Percentage of orders marked as Shipped or Completed.
	7. Average Shipping Time: Average days taken to ship orders.
	8. Sales by Product: Total revenue grouped by product_id.
	9. Sales by Customer: Revenue per customer.
	10. Sales Trends: Monthly and yearly breakdown of sales.
5.	Code Execution
    1. To execute the pipeline, run the mongodb_to_delta_lake.py script. This script performs the following:
    2. Connects to MongoDB Atlas to retrieve data.
    3. Processes data across the Bronze, Silver, and Gold layers.
    4. Calculates the required KPIs and saves the results in Delta tables for easy querying and reporting.
6.	Data Storage
	The final Delta Lake tables are stored in the specified directory (/FileStore/tables/Gold_Load/) as:
	gold_orders_analytics
	gold_sales_metrics
8. Additional Considerations
	Z-Ordering: Applied on frequently queried columns to optimize data retrieval in Delta Lake.
