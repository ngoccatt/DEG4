from pyflink.table import EnvironmentSettings, TableEnvironment
from flask import Flask
from flask_cors import CORS

from connector import cnt
from flink import flink_tables

env_setting = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_setting)

# flink_tables.createTable(table_env, cnt)
flink_tables.getAnalitics(table_env, cnt)

app = Flask(__name__)

CORS(app, origins='http://localhost:3000')


@app.route("/test")
def test():
    records = flink_tables.belong.fetch(5)
    result =  formatResult(True, records)
    return result

def formatResult(status, records):
    df = records.to_pandas()
    data = df.values.tolist()
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
    res = {"status": True}
    flink_tables.getAnalitics(table_env, cnt)
    return res

# doanh thu theo tung quoc gia
@app.route("/revenue/countries")
def revenue_report():
    update_analytics()
    result = "Empty"
    df = flink_tables.pdf_revenue
    df = df.groupby("country", as_index=False)[["revenue"]].sum()
    df = df.sort_values(by=("revenue"), ascending=False)
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#doanh thu theo tung thang
@app.route("/revenue/monthly")
def revenue_monthly():
    update_analytics()
    result = "Empty"
    df = flink_tables.pdf_revenue
    df["time"] = df["year"].astype("str") + '-' + df["month"].astype("str")
    df = df.groupby("time", as_index=False, sort=False)[["revenue"]].sum()
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#doanh thu theo tung nam
@app.route("/revenue/yearly")
def revenue_yearly():
    update_analytics()
    result = "Empty"
    df = flink_tables.pdf_revenue
    df = df.groupby("year", as_index=False, sort=False)[["revenue"]].sum()
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#so don hang theo tung quoc gia
@app.route("/orders/countries")
def orders_coun():
    update_analytics()
    result = "Empty"
    df = flink_tables.pdf_avg_revenue
    df = df.groupby("country", as_index=False, sort=False)[["num_orders"]].sum()
    df = df.sort_values(by=["num_orders"], ascending = False)
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#trung binh gia tri don hang theo tung quoc gia
@app.route("/orders/avg")
def orders_avg():
    update_analytics()
    result = "Empty"
    df = flink_tables.pdf_avg_revenue
    df = df[
        ["country", "num_orders", "total_revenue"]
        ].groupby("country", as_index=False, sort=False)[["num_orders", "total_revenue"]].sum()
    df["avg"] = df["total_revenue"] / df["num_orders"]
    df = df.sort_values(by=["avg"], ascending = False)
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result


#so don hang duoc dat theo khung gio
@app.route("/orders/timeslot")
def orders_timeslot():
    update_analytics()
    result = "Empty"
    df = flink_tables.pdf_order_timeslot
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#tong so don hang duoc dat theo khung gio (xuyen quoc gia)
@app.route("/orders/timeslot_sum")
def orders_timeslot_sum():
    update_analytics()
    result = "Empty"
    df = flink_tables.pdf_order_timeslot
    df = df.iloc[:, 1:]
    df = df.agg(["sum"], axis = 0)
    df = df.T.reset_index(names='timeslot').rename(columns = {"sum":"num_orders"})
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#tong so luong don hang bi huy tren tong don hang o tung quoc gia
@app.route("/orders/cancelled")
def orders_cancelled():
    update_analytics()
    result = "Empty"
    df = flink_tables.pdf_cancel_rate
    df = df[["total_orders", "cancel_orders"]]
    df["success_orders"] = df["total_orders"] - df["cancel_orders"]
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

#tong so luong don hang bi huy tren tong don hang
@app.route("/orders/cancelled_sum")
def orders_cancelled_sum():
    update_analytics()
    result = "Empty"
    df = flink_tables.pdf_cancel_rate
    print(df)
    df = df[["total_orders", "cancel_orders"]]
    print(df)
    df["success_orders"] = df["total_orders"] - df["cancel_orders"]
    df = df.drop(columns = ["total_orders"])
    df = df.iloc[:, 0:].agg(["sum"], axis = 0)
    df = df.T.reset_index(names='name').rename(columns = {"sum":"value"})
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.to_dict(orient='records')}
    return result

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000 ,debug=True)