from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from connector import *

env_setting = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_setting)

# Exact data
print("Exact data...")
my_source_ddl = """
    create table rawData (
        InvoiceNo VARCHAR(255),
        StockCode VARCHAR(255),
        Description VARCHAR(255),
        Quantity VARCHAR(255),
        InvoiceDate VARCHAR(255),
        UnitPrice VARCHAR(255),
        CustomerID VARCHAR(255),
        Country VARCHAR(255)
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '/ETL/src_data/data.csv'
    )
"""

my_clear_table_ddl = """
    create table clearData (
        InvoiceNo VARCHAR(255),
        StockCode VARCHAR(255),
        Description VARCHAR(255),
        Quantity VARCHAR(255),
        InvoiceDate VARCHAR(255),
        UnitPrice VARCHAR(255),
        CustomerID VARCHAR(255),
        Country VARCHAR(255),
        Canceled VARCHAR(255)
    ) 
"""

table_env.execute_sql(my_source_ddl).wait()
    
raw_data = table_env.from_path('rawData')

clear_data_stmt = """
    SELECT InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country, SUBSTRING(Quantity, 1, 1) AS Sign_Quantity, SUBSTRING(InvoiceNo, 1, 1) AS Cancel_Order FROM rawData"""

clear_data_1 =  table_env.sql_query(clear_data_stmt)

# clear_data_1.execute().print()

table_env.register_table("ClearDataTable1", clear_data_1)

clear_data_stmt = """
    SELECT CASE WHEN Cancel_Order = 'C' THEN 'TRUE' ELSE 'FALSE' END AS Cancelled, InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country FROM ClearDataTable1 WHERE Sign_Quantity <> '-' OR Cancel_Order = 'C'"""

clear_data_2 =  table_env.sql_query(clear_data_stmt)

table_env.register_table("FinalClearDataTable", clear_data_2)

# Transform data
print("Transform data...")
create_data_product_stmt = """SELECT StockCode AS ID, MAX(Description) AS Description, MAX(UnitPrice) AS UnitPrice FROM FinalClearDataTable WHERE StockCode <> ''  GROUP BY StockCode"""
create_data_customer_stmt = """SELECT CustomerID AS ID, MAX(Country) AS Country FROM FinalClearDataTable WHERE CustomerID <> '' GROUP BY CustomerID"""
create_data_order_stmt = """SELECT InvoiceNo AS ID, MAX(CustomerID) AS CustomerID, MAX(Cancelled) AS Cancelled,SUBSTRING(MIN(InvoiceDate), 1, locate(' ', MIN(InvoiceDate))) AS InvoiceDate, SUBSTRING(MIN(InvoiceDate), locate(' ', MIN(InvoiceDate)), 10) AS InvoiceTime FROM FinalClearDataTable WHERE InvoiceNo <> '' GROUP BY InvoiceNo"""
create_data_belong_stmt = """SELECT InvoiceNo AS OrderID, StockCode AS ProductID, MIN(Quantity) AS Quantity FROM FinalClearDataTable GROUP BY InvoiceNo, StockCode"""

products = table_env.sql_query(create_data_product_stmt)
customers = table_env.sql_query(create_data_customer_stmt)
orders = table_env.sql_query(create_data_order_stmt)
belongs = table_env.sql_query(create_data_belong_stmt)

products_pandas = products.to_pandas()
products_pandas = products_pandas[products_pandas.ID != 'StockCode']
customers_pandas = customers.to_pandas()
customers_pandas = customers_pandas[customers_pandas.ID != 'CustomerID']
orders_pandas = orders.to_pandas()
orders_pandas = orders_pandas[orders_pandas.ID != 'InvoiceNo']
belongs_pandas = belongs.to_pandas()
belongs_pandas = belongs_pandas[belongs_pandas.OrderID != 'InvoiceNo']

# Load data
print("Load data...")
cnt.upload_df(customers_pandas, "sample-bucket-trash", "customers.csv")
cnt.upload_df(orders_pandas, "sample-bucket-trash", "orders.csv")
cnt.upload_df(products_pandas, "sample-bucket-trash", "products.csv")
cnt.upload_df(belongs_pandas, "sample-bucket-trash", "belongs.csv")
    
cnt.loadDataToRedShift()

print("Finish ETL")