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
            redshift_connector.paramstyle = 'format'
            self.connector = connector
            self.cursor = connector.cursor()
            print("Connect to RedShift successfully")
        except Exception as e:
            print(e)
            
    def fetchData(self, stmt):
        try:
            self.cursor.execute(stmt)
            result: pandas.DataFrame = self.cursor.fetch_dataframe()
            while (len(result) < 1):
                self.cursor.execute(stmt)
                result: pandas.DataFrame = self.cursor.fetch_dataframe()
            
            
            return result
        except Exception as e:
            print(e)
            
    def fetchAll(self, stmt):
        try:
            self.cursor.execute(stmt)
            result: tuple = self.cursor.fetchall()
            return result
        except Exception as e:
            print(e)
            
    def insertData(self, table:str, column_name_list:list, value_list:list):
        names_str = ""
        values_str = ""
        if (len(column_name_list) != len(value_list)):
            raise Exception("columns length and values do not match")
        for name in column_name_list:
            names_str += f'{name},'
        for value in value_list:
            values_str += '%s,'

        try:
            stmt = f'INSERT INTO {table}({names_str[:-1]}) VALUES({values_str[:-1]})'
            print(stmt, value_list)
            self.cursor.execute(stmt, value_list)
        except Exception as e:
            print(e)
            
    def closeConnect(self):
        self.cursor.close()
        self.connector.close()
    
cnt = Connector()
    