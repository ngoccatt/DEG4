from pyflink.table import EnvironmentSettings, TableEnvironment
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from connector import cnt
from flink import flink_tables
from json import dumps
import atexit
import pandas as pd

env_setting = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_setting)

# flink_tables.createTable(table_env, cnt)
flink_tables.getAnalitics(table_env, cnt)

app = Flask(__name__)
CORS(app)

@app.route("/test")
def test():
    records = flink_tables.belong.fetch(5)
    result =  formatResult(True, records)
    return result

def formatResult(status, records):
    data = records.values.tolist()
    return {"status": status, "data": data}

@app.route("/testInsert")
def myInsert():
    name = ["name", "class", "school"]
    value = ["x", 8, "x"]
    cnt.insertData("temp", name, value)
    
    # sql = f'INSERT INTO temp(name,class,school) VALUES(%s,%s,%s)'
    # cnt.cursor.execute(sql, value)
    return {"status": True}

@app.route("/updateAnalytics")
def update_analytics():
    res = {"status": False}
    res["status"] = flink_tables.getAnalitics(table_env, cnt)
    return res

# doanh thu theo tung quoc gia
@app.route("/revenue/countries")
def revenue_report():
    
    result = "Empty"
    df = pd.DataFrame(flink_tables.pdf_revenue)
    df = df.groupby("country", as_index=False)[["revenue"]].sum()
    df = df.sort_values(by=("revenue"), ascending=False)
    print(df.head())
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#doanh thu theo tung thang
@app.route("/revenue/monthly")
def revenue_monthly():
    
    result = "Empty"
    df = pd.DataFrame(flink_tables.pdf_revenue)
    df["time"] = df["year"].astype("str") + '-' + df["month"].astype("str")
    df = df.groupby("time", as_index=False, sort=False)[["revenue"]].sum()
    print(df.head())
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#doanh thu theo tung nam
@app.route("/revenue/yearly")
def revenue_yearly():
    
    result = "Empty"
    df = pd.DataFrame(flink_tables.pdf_revenue)
    df = df.groupby("year", as_index=False, sort=False)[["revenue"]].sum()
    print(df.head())
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#so don hang theo tung quoc gia
@app.route("/orders/countries")
def orders_coun():
    
    result = "Empty"
    df = pd.DataFrame(flink_tables.pdf_avg_revenue)
    print(df.head())
    df = df.groupby("country", as_index=False, sort=False)[["num_orders"]].sum()
    df = df.sort_values(by=["num_orders"], ascending = False)
    print(df.head())
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#trung binh gia tri don hang theo tung quoc gia
@app.route("/orders/avg")
def orders_avg():
    
    result = "Empty"
    df = pd.DataFrame(flink_tables.pdf_avg_revenue)
    df = df[
        ["country", "num_orders", "total_revenue"]
        ].groupby("country", as_index=False, sort=False)[["num_orders", "total_revenue"]].sum()
    df["avg"] = df["total_revenue"] / df["num_orders"]
    df = df.sort_values(by=["avg"], ascending = False)
    print(df.head())
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result


#so don hang duoc dat theo khung gio
@app.route("/orders/timeslot")
def orders_timeslot():
    
    result = "Empty"
    df = pd.DataFrame(flink_tables.pdf_order_timeslot)
    print(df.head())
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#tong so don hang duoc dat theo khung gio (xuyen quoc gia)
@app.route("/orders/timeslot_sum")
def orders_timeslot_sum():
    
    result = "Empty"
    df = pd.DataFrame(flink_tables.pdf_order_timeslot)
    df = df.iloc[:, 1:]
    df = df.agg(["sum"], axis = 0)
    df = df.T.reset_index(names='timeslot').rename(columns = {"sum":"num_orders"})
    print(df.head())
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#tong so luong don hang bi huy tren tong don hang o tung quoc gia
@app.route("/orders/cancelled")
def orders_cancelled():
    
    result = "Empty"
    df = pd.DataFrame(flink_tables.pdf_cancel_rate)
    df = df[["total_orders", "cancel_orders"]]
    df["success_orders"] = df["total_orders"] - df["cancel_orders"]
    print(df.head())
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#tong so luong don hang bi huy tren tong don hang
@app.route("/orders/cancelled_sum")
def orders_cancelled_sum():
    
    result = "Empty"
    df = pd.DataFrame(flink_tables.pdf_cancel_rate)
    print(df.head())
    df = df[["total_orders", "cancel_orders"]]
    print(df.head())
    df["success_orders"] = df["total_orders"] - df["cancel_orders"]
    df = df.drop(columns = ["total_orders"])
    df = df.iloc[:, 0:].agg(["sum"], axis = 0)
    df = df.T.reset_index(names='name').rename(columns = {"sum":"value"})
    print(df.head())
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

@app.route("/api/v1/total_product")
def getTotalProduct():
    try: 
        total_product = flink_tables.getToTalProduct(cnt)
        result = {"status": True, "data": total_product}
        return jsonify(result)
        # return formatResult(True, total_product)
    except Exception as e:
        print(e)
        return {"status": False}
    
@app.route("/api/v1/paginate_product")
def paginateProduct():
    try:
        offset = request.args.get("offset")
        limit_number = request.args.get("limit")
        current_product = flink_tables.paginateProduct(offset, limit_number, cnt)
        result = {"status": True, "data": current_product}
        return jsonify(result)
    except Exception as e:
        print(e)
        return {"status": False}
    

@app.route("/api/v1/product_info")
def getAllProduct():
    try:
        all_product = flink_tables.getAllProduct(cnt)
        result = {"status": True, "products": all_product}
        return jsonify(result)
    except Exception as e:
        print(e)
        return {"status": False}  
    
@app.route("/api/v1/bestseller")
def getBestSeller():
    try:
        all_product = flink_tables.getBestSellerProduct(cnt)
        result = {"status": True, "bestseller": all_product}
        return jsonify(result)
    except Exception as e:
        print(e)
        return {"status": False} 

@app.route("/api/v1/order", methods=['POST'])
def makeOrder():
    try:
        request_data = request.get_json()
        orderId = request_data["orderId"]
        productId = request_data["productId"]
        customerId = request_data["customerId"]
        all_product = flink_tables.createOrder(orderId, customerId, productId, cnt)
        return jsonify({"status": True, "bestseller": all_product})
    except Exception as e:
        print(e)
        return {"status": False}    
    
def disconnectDB():
    try:
        cnt.closeConnect()
    except Exception as e:
        print(e)

atexit.register(disconnectDB)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000 ,debug=True)