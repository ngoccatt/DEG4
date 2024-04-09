from connector import *
import logging
import boto3
from botocore.exceptions import ClientError
import os
import pandas as pd

# upload csv files to s3

files = [
    "customers.csv",
    "orders.csv",
    "products.csv",
    "belongs.csv"
]

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3',aws_access_key_id = "AKIA6ODU3JISZYOLPMEY",
                             aws_secret_access_key = "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")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_df(df: pd.DataFrame, bucket_name, object_name):

    s3_client = boto3.client('s3',aws_access_key_id = "AKIA6ODU3JISZYOLPMEY",
                             aws_secret_access_key = "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")
    try:
        response = s3_client.put_object(Bucket = bucket_name,
                                        Body = bytes(df.to_csv(), 'utf-8'), 
                                        Key = object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# if data is upload directly to S3 from dataframe, the below lines are not necessary

print("Uploading file to S3...")

upload_file(f"/ETL/sink_data/{files[0]}", "sample-bucket-trash")
upload_file(f"/ETL/sink_data/{files[1]}", "sample-bucket-trash")
upload_file(f"/ETL/sink_data/{files[2]}", "sample-bucket-trash")
upload_file(f"/ETL/sink_data/{files[3]}", "sample-bucket-trash")

print("Completed uploading file to S3.")

# ---------------------------------------------------------------------------------

my_cnt = Connector()

# create temporary table:
create_tables_sql = []
create_tables_sql.append(
"""
CREATE TEMP TABLE IF NOT EXISTS temp_Customer(
    ID INTEGER not null,
    Country VARCHAR(30),
    PRIMARY KEY(ID)
    )
    DISTKEY(ID);
"""
)

create_tables_sql.append(
"""
CREATE TEMP TABLE IF NOT EXISTS temp_Orders(
    ID VARCHAR(7) not null,
    CustomerID INTEGER,
    Cancelled boolean,
    InvoiceDate date not null,
    InvoiceTime time not null,
    PRIMARY KEY(ID)
    )
    DISTKEY(ID);
"""
)

create_tables_sql.append(
"""
CREATE TEMP TABLE IF NOT EXISTS temp_Product(
    ID VARCHAR(15) not null,
    Description VARCHAR(100),
    UnitPrice REAL not null,
    PRIMARY KEY(ID)
    )
    DISTKEY(ID);
"""
)

create_tables_sql.append(
"""
CREATE TEMP TABLE IF NOT EXISTS temp_Belong(
    OrderID VARCHAR(7) not null,
    ProductID VARCHAR(15) not null,
    Quantity INTEGER,
    PRIMARY KEY(OrderID, ProductID)
    )
    DISTKEY(OrderID);
"""
)

copy_commands = []
copy_commands.append(f"""
    COPY temp_Customer 
    FROM 's3://sample-bucket-trash/{files[0]}' 
    FORMAT CSV
    IGNOREHEADER 1
    IAM_ROLE 'arn:aws:iam::992382503461:role/service-role/AmazonRedshift-CommandsAccessRole-20240326T181052';
    """)

copy_commands.append(f"""
    COPY temp_Orders 
    FROM 's3://sample-bucket-trash/{files[1]}' 
    FORMAT CSV
    IGNOREHEADER 1
    DATEFORMAT 'MM/DD/YYYY'
    TIMEFORMAT 'auto'
    IAM_ROLE 'arn:aws:iam::992382503461:role/service-role/AmazonRedshift-CommandsAccessRole-20240326T181052';
""")

copy_commands.append(f"""
    COPY temp_Product 
    FROM 's3://sample-bucket-trash/{files[2]}'  
    IGNOREHEADER 1
    FORMAT CSV
    IAM_ROLE 'arn:aws:iam::992382503461:role/service-role/AmazonRedshift-CommandsAccessRole-20240326T181052';
""")

copy_commands.append(f"""
    COPY temp_belong 
    FROM 's3://sample-bucket-trash/{files[3]}' 
    DELIMITER ',' 
    IGNOREHEADER 1
    IAM_ROLE 'arn:aws:iam::992382503461:role/service-role/AmazonRedshift-CommandsAccessRole-20240326T181052';
""")

merge_commands = []

merge_commands.append("""
    MERGE INTO customer
    USING temp_customer
    ON customer.id = temp_customer.id
    REMOVE DUPLICATES;
""")

merge_commands.append("""
    MERGE INTO orders
    using temp_orders
    ON orders.id = temp_orders.id
    REMOVE DUPLICATES;
""")

merge_commands.append("""
    MERGE INTO product
    using temp_product
    on product.id = temp_product.id
    REMOVE DUPLICATES;
""")

merge_commands.append("""
    MERGE INTO belong
    using temp_belong
    on belong.orderid = temp_belong.orderid 
    AND belong.productid = temp_belong.productid
    REMOVE DUPLICATES;
""")



for i in range(len(files)):
    my_cnt.cursor.execute(create_tables_sql[i])
    print(f"Created temporary tables {i}")
    my_cnt.cursor.execute(copy_commands[i])
    print(f"Copy data from s3 to temporary tables {i}")
    my_cnt.cursor.execute(merge_commands[i])
    print(f"Merged the temporary tables {i} with the real tables")

print("Complete loading data")