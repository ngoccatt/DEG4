from pyflink.table import DataTypes

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
            
    def getAnalitics(self, table_env, cnt):
        if not self.update:
            return 
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
            
            print("Fetched all table successfully")
            self.update = False

        except Exception as e:
            print(e)
            
flink_tables = Flink()
