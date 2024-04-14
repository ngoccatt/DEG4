from pyflink.table import DataTypes
from datetime import date, datetime

class Flink:
    def __init__(self): 
        self.update = True
    
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
                                DataTypes.FIELD("cancelled", DataTypes.BOOLEAN()),
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
            
    def setUpdate(self, update):
        self.update = update
            
    def getAnalitics(self, table_env, cnt):
        if not self.update:
            return False
        try: 
            self.pdf_revenue = cnt.fetchData(
                                        """
                    select country, (date_part(year, invoiceDate)) as year, 
                        (date_part(month, invoiceDate)) as month, 
                        sum(unitprice * quantity) as revenue
                    from orders, belong, product, customer
                    where orders.cancelled = FALSE
                    and belong.orderid = orders.id
                    and belong.productid = product.id
                    and orders.customerid = customer.id
                    group by country, year, month
                    order by 1, 2, 3;
                                        """
                                        )
            
            self.pdf_avg_revenue = cnt.fetchData(
                                        """
                    SELECT 
                        country, 
                        DATE_PART('year', invoiceDate) AS year, 
                        DATE_PART('month', invoiceDate) AS month, 
                        COUNT(DISTINCT orders.id) AS num_orders,
                        SUM(unitprice * quantity) AS total_revenue,
                        SUM(unitprice * quantity) / COUNT(DISTINCT orders.id) AS avg_price_per_order
                    FROM 
                        orders
                    JOIN 
                        belong ON belong.orderid = orders.id
                    JOIN 
                        product ON belong.productid = product.id
                    JOIN 
                        customer ON orders.customerid = customer.id
                    WHERE 
                        orders.cancelled = FALSE
                    GROUP BY 
                        country, year, month
                    ORDER BY 
                        country, year, month, num_orders;
                                        """
                                        )
            
            self.pdf_order_timeslot = cnt.fetchData(
                                        """
                    SELECT 
                        country,
                        COUNT(DISTINCT CASE WHEN EXTRACT('hour' FROM invoiceTime) BETWEEN 0 AND 2 THEN orders.id END) AS h0_3,
                        COUNT(DISTINCT CASE WHEN EXTRACT('hour' FROM invoiceTime) BETWEEN 3 AND 5 THEN orders.id END) AS h3_6,
                        COUNT(DISTINCT CASE WHEN EXTRACT('hour' FROM invoiceTime) BETWEEN 6 AND 8 THEN orders.id END) AS h6_9,
                        COUNT(DISTINCT CASE WHEN EXTRACT('hour' FROM invoiceTime) BETWEEN 9 AND 11 THEN orders.id END) AS h9_12,
                        COUNT(DISTINCT CASE WHEN EXTRACT('hour' FROM invoiceTime) BETWEEN 12 AND 14 THEN orders.id END) AS h12_15,
                        COUNT(DISTINCT CASE WHEN EXTRACT('hour' FROM invoiceTime) BETWEEN 15 AND 17 THEN orders.id END) AS h15_18,
                        COUNT(DISTINCT CASE WHEN EXTRACT('hour' FROM invoiceTime) BETWEEN 18 AND 20 THEN orders.id END) AS h18_21,
                        COUNT(DISTINCT CASE WHEN EXTRACT('hour' FROM invoiceTime) BETWEEN 21 AND 23 THEN orders.id END) AS h21_24
                    FROM 
                        orders
                    JOIN 
                        customer ON orders.customerid = customer.id
                    WHERE 
                        orders.cancelled = FALSE
                    GROUP BY 
                        country
                    ORDER BY 
                        h12_15 DESC, country;
                                        """
                                        )
            
            self.pdf_cancel_rate = cnt.fetchData(
                                        """
                    SELECT 
                        country,
                        COUNT(DISTINCT CASE cancelled WHEN TRUE THEN orders.id END) AS cancel_orders,
                        COUNT(DISTINCT orders.id) as total_orders
                    FROM 
                        orders
                    JOIN 
                        customer ON orders.customerid = customer.id
                    GROUP BY 
                        country
                    ORDER BY 
                        total_orders DESC, country ;
                                        """
                                        )
            if (
                self.pdf_revenue.empty or
                self.pdf_avg_revenue.empty or
                self.pdf_order_timeslot.empty or
                self.pdf_cancel_rate.empty
            ):
                print("Failed to fetch all table. Please refresh the page")
                self.update = True
            else:
                print("Fetched all table successfully")
                self.update = False
            return True
        except Exception as e:
            print(e)
            
    def getToTalProduct(self, cnt):
        stmt = "SELECT COUNT(id) as total_product FROM product;"
        total_product = cnt.fetchAll(stmt)
        return total_product
        
    def paginateProduct(self, offset, limit, cnt):
        stmt = f"SELECT * FROM product LIMIT {limit} OFFSET {offset};"
        currentProduct = cnt.fetchAll(stmt)
        return currentProduct
    
    def getAllProduct(self, cnt):
        stmt = f"SELECT * FROM product;"
        currentProduct = cnt.fetchAll(stmt)
        return currentProduct
    
    def getBestSellerProduct(self, cnt):
        stmt = f"SELECT  product.id AS ID, MAX(product.description) AS description, MAX(product.unitprice) AS unitprice, COUNT(product.id) AS num_sale FROM orders JOIN belong ON orders.id = belong.orderid JOIN product ON  product.id = belong.productid WHERE orders.cancelled <> true GROUP BY product.id ORDER BY COUNT(product.id) DESC LIMIT 16"
        currentBestSeller = cnt.fetchAll(stmt)
        return currentBestSeller
    
    def createOrder(self, orderId, customerId, productId, cnt):
        current_date= datetime.now()
        current_time = current_date.strftime('%H:%M:%S')
        cnt.insertData("orders", ["id", "customerid", "cancelled", "invoicedate", "invoicetime"], [orderId, customerId, False, date.today(), current_time])
        cnt.insertData("belong", ["orderid", "productid", "quantity"], [orderId, productId, 1])
        currentBestSeller = self.getBestSellerProduct(cnt)
        self.setUpdate(True)
        return currentBestSeller
        
            
flink_tables = Flink()
