
from flask import Flask, request, jsonify
from google.cloud import storage
from datetime import datetime, timezone
import uuid
import json
import os
import random

app = Flask(__name__)

BUCKET_NAME = os.environ.get(
    "BUCKET_NAME", "personalproject_1"
)

# ==========================================
# COMMON FUNCTIONS
# ==========================================

def rid(prefix, index):
    return f"{prefix}_{index + 1:06}"


def upload_json(data, folder):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S"
    )

    # New naming convention
    filename = (
        f"{folder}/api_{timestamp}_"
        f"{uuid.uuid4().hex[:8]}.json"
    )

    # Convert records into BigQuery-compatible NDJSON
    if isinstance(data, list):
        json_content = "\n".join(
            json.dumps(record) for record in data
        )
    else:
        json_content = json.dumps(data)

    blob = bucket.blob(filename)

    blob.upload_from_string(
        json_content,
        content_type="application/x-ndjson",
        if_generation_match=0
    )

    print(f"Uploaded NDJSON file: {filename}", flush=True)

    return filename


# ==========================================
# 1. EXISTING HTTP INGESTION
# ==========================================

@app.route("/ingest", methods=["POST"])
def ingest():

    data = request.get_json(silent=True)

    if data is None:
        return jsonify({
            "error": "Valid JSON required"
        }), 400

    filename = upload_json(
        data,
        "landing/api"
    )

    return jsonify({
        "status": "success",
        "file": filename
    }), 201


# ==========================================
# 2. GENERATE 2000 CLINICAL JSON RECORDS
# ==========================================

@app.route("/generate", methods=["POST"])
def generate():

    rng = random.Random()
    records = []

    conditions = [
        "Hypertension",
        "Diabetes",
        "Asthma",
        "Migraine",
        "Arthritis"
    ]

    treatments = [
        "Treatment A",
        "Treatment B",
        "Placebo"
    ]

    for i in range(2000):

        patient_index = rng.randrange(7000)

        site_index = patient_index % 1500
        study_index = site_index % 1000

        record = {
            "event_id": str(uuid.uuid4()),
            "patient_id": rid(
                "PAT", patient_index
            ),
            "study_id": rid(
                "STUDY", study_index
            ),
            "site_id": rid(
                "SITE", site_index
            ),
            "enrollment_id": rid(
                "ENR", patient_index
            ),
            "age": rng.randint(18, 85),
            "condition": rng.choice(conditions),
            "treatment_arm": rng.choice(treatments),
            "event_type": rng.choice([
                "Patient Update",
                "Follow-up",
                "Treatment Update",
                "Clinical Observation"
            ]),
            "event_status": rng.choice([
                "Completed",
                "Pending",
                "In Progress"
            ]),
            "event_timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "source_system": "FLASK_API"
        }

        records.append(record)

    filename = upload_json(
        records,
        "landing/api"
    )

    return jsonify({
        "status": "success",
        "total_records": len(records),
        "file": filename
    }), 201


# ==========================================
# 3. HEALTH CHECK
# ==========================================

@app.route("/", methods=["GET"])
def home():
    return "Clinical Ingress Running", 200
