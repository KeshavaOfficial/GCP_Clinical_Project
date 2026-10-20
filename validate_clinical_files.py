
import csv
import io
from google.cloud import storage

BUCKET_NAME = "personalproject_1"

EXPECTED_FILES = [
    "adverse_events.csv",
    "clinical_sites.csv",
    "clinical_studies.csv",
    "investigators.csv",
    "lab_results.csv",
    "medical_history.csv",
    "medications.csv",
    "patient_enrollments.csv",
    "patient_visits.csv",
    "patients.csv",
    "study_treatments.csv",
    "vital_signs.csv",
]


def validate_clinical_files(batch_id, **kwargs):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    prefix = f"clinical_source/{batch_id}/"
    print(f"Validating files under gs://{BUCKET_NAME}/{prefix}")

    for filename in EXPECTED_FILES:
        blob = bucket.blob(prefix + filename)

        if not blob.exists():
            raise FileNotFoundError(
                f"Missing clinical file: {filename}"
            )

        blob.reload()

        if not blob.size or blob.size == 0:
            raise ValueError(
                f"Empty clinical file: {filename}"
            )

        # Read only the header, not the entire CSV.
        with blob.open("r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader, None)

        if not header:
            raise ValueError(
                f"Missing CSV header: {filename}"
            )

        columns = [col.strip() for col in header]

        if any(not col for col in columns):
            raise ValueError(
                f"Blank column name: {filename}"
            )

        if len(columns) != len(set(columns)):
            raise ValueError(
                f"Duplicate columns: {filename}"
            )

        print(
            f"VALID: {filename} | "
            f"Columns: {len(columns)} | "
            f"Size: {blob.size} bytes"
        )

    print("SUCCESS: All 12 clinical CSV files validated.")
