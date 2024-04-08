from pyflink.table import EnvironmentSettings, TableEnvironment
from flask import Flask

from connector import cnt
from flink import flink_tables

env_setting = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_setting)

flink_tables.createTable(table_env, cnt)

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


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000 ,debug=True)