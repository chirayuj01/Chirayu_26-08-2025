from flask import Flask, jsonify, request, send_file
import uuid, sqlite3
from db import init_db, get_connection
from compute import compute_report
import os
import subprocess

DB_FILE = "store.db"
app = Flask(__name__)

if not os.path.exists(DB_FILE):
    subprocess.run(["python", "ingest.py"])

init_db()

@app.route("/")
def home():
    return {"message": "API running with database ready!"}

@app.route("/trigger_report", methods=["POST"])
def trigger():
    report_id = str(uuid.uuid4())
    try:
        csv_path = compute_report()
        conn = get_connection()
        conn.execute("INSERT INTO reports VALUES (?,?,?)",
                     (report_id, "Complete", csv_path))
        conn.commit(); conn.close()
        return jsonify({"report_id": report_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get_report", methods=["GET"])
def get_report():
    report_id = request.args.get("report_id")
    conn = get_connection()
    row = conn.execute("SELECT status,csv_path FROM reports WHERE report_id=?",
                       (report_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error":"report not found"}),404
    if row["status"]=="Running":
        return jsonify({"status":"Running"})
    return send_file(row["csv_path"], as_attachment=True)

