import pandas
import redshift_connector

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
            result: pandas.DataFrame = self.cursor.fetch_dataframe()
            
            return result
        except Exception as e:
            print(e)
            
    def closeConnect(self):
        self.cursor.close()
        self.connector.close()
    
cnt = Connector()
    