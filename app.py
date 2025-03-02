from flask import Flask, jsonify, request
import pandas as pd
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

MONGO_URI = "mongodb://codelab1:neucodelab@101.96.66.219:8000/?authMechanism=SCRAM-SHA-1"
client = MongoClient(MONGO_URI)
db = client["vncodelab"]
collection = db["logs"]

def process_logs(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])  # Loại bỏ dòng có timestamp lỗi
    df["week"] = df["timestamp"].dt.to_period("W").apply(lambda r: r.start_time.strftime("%Y-%m-%d"))

    # Xác định cột tên người dùng: Ưu tiên 'user', nếu không có thì lấy 'userName'
    if "user" in df.columns:
        df["user_final"] = df["user"].fillna(df["userName"])
    else:
        df["user_final"] = df["userName"]

    df = df.dropna(subset=["user_final"])  # Loại bỏ dòng không có user nào cả

    activity_by_week = df.groupby(["week", "user_final"]).size().reset_index(name="activity_count")
    afk_by_week = df[df["logType"] == "leaveRoom"].groupby(["week", "user_final"]).size().reset_index(name="afk_count")

    merged_data = pd.merge(activity_by_week, afk_by_week, on=["week", "user_final"], how="left").fillna(0)
    merged_data["afk_count"] = merged_data["afk_count"].astype(int)
    merged_data["real_activity"] = merged_data["activity_count"] - merged_data["afk_count"]

    max_real_activity_per_week = merged_data.groupby("week")["real_activity"].transform("max")
    merged_data["hardworking_score"] = (merged_data["real_activity"] / max_real_activity_per_week) * 100
    merged_data["hardworking_score"] = merged_data["hardworking_score"].fillna(0).astype(int)

    output_json = {}
    for _, row in merged_data.iterrows():
        week = row["week"]
        user = row["user_final"]
        if week not in output_json:
            output_json[week] = {}
        output_json[week][user] = {
            "activity_count": int(row["activity_count"]),
            "afk_count": int(row["afk_count"]),
            "real_activity": int(row["real_activity"]),
            "hardworking_score": int(row["hardworking_score"]),
        }
    return output_json

@app.route("/api/hardworking", methods=["GET"])
def get_hardworking_data():
    df = pd.read_csv("vncodelab.logs.csv")
    return jsonify(process_logs(df))

@app.route("/api/hardworking2", methods=["GET"])
def get_hardworking_data2():
    room_id = request.args.get("roomID")
    if not room_id:
        return jsonify({"error": "Thiếu roomID"}), 400

    data = list(collection.find({"roomID": room_id}, {"_id": 0}))  # Loại bỏ ObjectId để tránh lỗi

    # Debug dữ liệu MongoDB
    print("Dữ liệu từ MongoDB:", data)

    df = pd.DataFrame(data)
    if df.empty:
        return jsonify({})

    return jsonify(process_logs(df))

if __name__ == "__main__":
    app.run(debug=True)
