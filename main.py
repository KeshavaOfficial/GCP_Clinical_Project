
from flask import Flask, request, jsonify
from google.cloud import storage
from datetime import datetime, timezone
import json
import uuid
import os

app = Flask(__name__)

BUCKET_NAME = os.environ.get(
    "BUCKET_NAME", "cliniccustomerbucket"
)

@app.route("/ingest", methods=["POST"])
def ingest_data():
    data = request.get_json(silent=True)

    if data is None:
        return jsonify({"error": "Valid JSON required"}), 400

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S"
    )
    filename = f"landing/clinical_{timestamp}_{uuid.uuid4().hex}.json"

    blob = bucket.blob(filename)
    blob.upload_from_string(
        json.dumps(data),
        content_type="application/json"
    )

    return jsonify({
        "status": "success",
        "file": filename
    }), 201

@app.route("/", methods=["GET"])
def health():
    return "Ingress Service Running", 200
