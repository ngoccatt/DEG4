import redshift_connector
import logging
import boto3
from botocore.exceptions import ClientError
import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

aws_access_key_id_ =  os.environ.get("aws_access_key_id")
aws_secret_access_key_ =  os.environ.get("aws_secret_access_key")

class Connector:
    def __init__(self):
        try:
            connector = redshift_connector.connect(
                host="compu-workgroup.992382503461.ap-southeast-2.redshift-serverless.amazonaws.com",
                is_serverless=True,
                port=5439,
                database='dev',
                user='admin',
                password='Doraemon12!'
            )
            connector.autocommit = True
            self.connector = connector
            self.cursor = connector.cursor()
        except Exception as e:
            print(e)
            
    def fetchData(self, stmt):
        try:
            self.cursor.execute(stmt)
            result: pd.DataFrame = self.cursor.fetch_dataframe()
            
            return result
        except Exception as e:
            print(e)
    
    def loadDataToRedShift(self):
        files = [
            "customers.csv",
            "orders.csv",
            "products.csv",
            "belongs.csv"
        ]
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
            self.cursor.execute(create_tables_sql[i])
            print(f"Created temporary tables {i}")
            self.cursor.execute(copy_commands[i])
            print(f"Copy data from s3 to temporary tables {i}")
            self.cursor.execute(merge_commands[i])
            print(f"Merged the temporary tables {i} with the real tables")

        print("Complete loading data")
        
    def upload_df(self, df: pd.DataFrame, bucket_name, object_name):

        s3_client = boto3.client('s3',aws_access_key_id = aws_access_key_id_,
                                aws_secret_access_key = aws_secret_access_key_)
        try:
            response = s3_client.put_object(Bucket = bucket_name,
                                            Body = bytes(df.to_csv(index=False), 'utf-8'), 
                                            Key = object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def closeConnect(self):
        self.cursor.close()
        self.connector.close()
    
cnt = Connector()