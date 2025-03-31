from flask import Flask, jsonify, request
import pandas as pd
from flask_cors import CORS 
from pymongo import MongoClient

app= Flask(__name__)
CORS(app)   

MONGO_URI = "mongodb://codelab1:neucodelab@101.96.66.219:8000/?authMechanism=SCRAM-SHA-1"
client = MongoClient(MONGO_URI)
db = client["vncodelab"]
collection = db["logs"]

def process_logs(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])  
    df["week"] = df["timestamp"].dt.to_period("W").apply(lambda r: r.start_time.strftime("%Y-%m-%d"))

    if "user" in df.columns:
        df["user_final"] = df["user"].fillna(df["userName"])
    else:
        df["user_final"] = df["userName"]

    df = df.dropna(subset=["user_final"]) 

    submit_code_df = df[df["logType"] == "codeSubmit"]

    submit_code_status_count = submit_code_df.groupby(["week", "user_final", "roomID", "log.status"]).size().unstack(fill_value=0)

    submit_code_status_count.columns = [f"submit_code_{col}" for col in submit_code_status_count.columns]

    output_json = {}
    for _, row in submit_code_status_count.iterrows():
        week = row.name[0]
        user = row.name[1]
        room_id = row.name[2]

        if week not in output_json:
            output_json[week] = {}
        if room_id not in output_json[week]:
            output_json[week][room_id] = {}

        output_json[week][room_id][user] = {
            "submit_code_accept": int(row.get("submit_code_accept",0)),
            "submit_code_error": int(row.get("submit_code_error",0)),
        }

    return output_json


@app.route("/api/submitcode", methods=["GET"])
def get_submitcode_data():
    room_id = request.args.get("roomID")
    if not room_id:
        return jsonify({"error": "Thiếu roomID"}), 400

    data = list(collection.find({"roomID": room_id}, {"_id": 0}))  

    print("Dữ liệu từ MongoDB:", data)

    df = pd.DataFrame(data)
    if df.empty:
        return jsonify({})

    return jsonify(process_logs(df))

if __name__ == "__main__":
    app.run(debug=True)


