# from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment
from flask import Flask

from connector import cnt
from flink import flink_tables

env_setting = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_setting)

flink_tables.createTable(table_env, cnt)
flink_tables.getAnalitics(table_env, cnt)

app = Flask(__name__)

@app.route("/test")
def test():
    records = flink_tables.belong.fetch(5)
    result =  formatResult(True, records)
    return result

def formatResult(status, records):
    df = records.to_pandas()
    data = df.values.tolist()
    return {"status": status, "data": data}

# doanh thu theo tung quoc gia
@app.route("/revenue/countries")
def revenue_report():
    result = "Empty"
    df = flink_tables.pdf_revenue
    df = df.groupby("country", as_index=False)[["revenue"]].sum()
    df = df.sort_values(by=("revenue"), ascending=False)
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.values.tolist()}
    return result

#doanh thu theo tung thang
@app.route("/revenue/monthly")
def revenue_monthly():
    result = "Empty"
    df = flink_tables.pdf_revenue
    df["time"] = df["year"].astype("str") + '-' + df["month"].astype("str")
    df = df.groupby("time", as_index=False, sort=False)[["revenue"]].sum()
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.values.tolist()}
    return result

#doanh thu theo tung nam
@app.route("/revenue/yearly")
def revenue_yearly():
    result = "Empty"
    df = flink_tables.pdf_revenue
    df = df.groupby("year", as_index=False, sort=False)[["revenue"]].sum()
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.values.tolist()}
    return result

#so don hang theo tung quoc gia
@app.route("/orders/countries")
def orders_coun():
    result = "Empty"
    df = flink_tables.pdf_avg_revenue
    df = df.groupby("country", as_index=False, sort=False)[["num_orders"]].sum()
    df = df.sort_values(by=["num_orders"], ascending = False)
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.values.tolist()}
    return result

#trung binh gia tri don hang theo tung quoc gia
@app.route("/orders/avg")
def orders_avg():
    result = "Empty"
    df = flink_tables.pdf_avg_revenue
    df = df[
        ["country", "num_orders", "total_revenue"]
        ].groupby("country", as_index=False, sort=False)[["num_orders", "total_revenue"]].sum()
    df["avg"] = df["total_revenue"] / df["num_orders"]
    df = df.sort_values(by=["avg"], ascending = False)
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.values.tolist()}
    return result


#so don hang duoc dat theo khung gio
@app.route("/orders/timeslot")
def orders_timeslot():
    result = "Empty"
    df = flink_tables.pdf_order_timeslot
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.values.tolist()}
    return result

#tong so don hang duoc dat theo khung gio (xuyen quoc gia)
@app.route("/orders/timeslot_sum")
def orders_timeslot_sum():
    result = "Empty"
    df = flink_tables.pdf_order_timeslot
    df = df.iloc[:, 1:]
    df = df.agg(["sum"], axis = 0)
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.values.tolist()}
    return result

#tong so luong don hang bi huy tren tong don hang o tung quoc gia
@app.route("/orders/cancelled")
def orders_cancelled():
    result = "Empty"
    df = flink_tables.pdf_cancel_rate
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.values.tolist()}
    return result

#tong so luong don hang bi huy tren tong don hang
@app.route("/orders/cancelled_sum")
def orders_cancelled_sum():
    result = "Empty"
    df = flink_tables.pdf_cancel_rate
    df = df.iloc[:, 1:].agg(["sum"], axis = 0)
    print(df)
    result = {"status": True, "header": df.columns.tolist(), "data": df.values.tolist()}
    return result

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000 ,debug=True)