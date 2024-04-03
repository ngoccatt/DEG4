from pyflink.table import DataTypes

class Flink:
    def __init__(self): pass
    
    def createTable(self, table_env, cnt):
        try: 
            pdf_belong = cnt.fetchData("select * from belong")
            pdf_orders = cnt.fetchData("select * from orders")
            pdf_customer = cnt.fetchData("select * from customer")
            pdf_product = cnt.fetchData("select * from product")
            
            print("Create belong table ...")
            self.belong = table_env.from_pandas(pdf_belong,
                          DataTypes.ROW([DataTypes.FIELD("orderId", DataTypes.STRING()),
                                         DataTypes.FIELD("productId", DataTypes.STRING()),
                                         DataTypes.FIELD("quantity", DataTypes.INT())]))
            
            print("Create orders table ...")
            self.orders = table_env.from_pandas(pdf_orders,
                DataTypes.ROW([DataTypes.FIELD("id", DataTypes.STRING()),
                                DataTypes.FIELD("customerId", DataTypes.INT()),
                                DataTypes.FIELD("invoicedate", DataTypes.DATE()),
                                DataTypes.FIELD("invoicetime", DataTypes.TIME())]))
            
            print("Create customer table ...")
            self.customer = table_env.from_pandas(pdf_customer,
                DataTypes.ROW([DataTypes.FIELD("id", DataTypes.INT()),
                                DataTypes.FIELD("country", DataTypes.STRING())]))
            
            print("Create product table ...")
            self.product = table_env.from_pandas(pdf_product,
                DataTypes.ROW([DataTypes.FIELD("id", DataTypes.STRING()),
                                DataTypes.FIELD("description", DataTypes.STRING()),
                                DataTypes.FIELD("unitprice", DataTypes.FLOAT())]))
            
            print("Create all table successfully")

        except Exception as e:
            print(e)
            
flink_tables = Flink()