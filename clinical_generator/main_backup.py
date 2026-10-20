from faker import Faker
from google.cloud import storage
import random
import csv
import os
import tempfile
import uuid
from datetime import datetime, timezone

# ==========================================
# 1. CONFIGURATION
# ==========================================

random.seed(42)
Faker.seed(42)
f = Faker()

PROJECT_ID = "vaarahigcpproject"

BUCKET_NAME = os.environ.get(
    "BUCKET_NAME", "personalproject_1"
)

# Temporary directory inside Cloud Run
base = tempfile.mkdtemp(prefix="clinical_data_")

# Use the Airflow-provided batch ID when available.
# Otherwise generate a unique ID for manual executions.

run_id = os.environ.get("BATCH_ID")

if not run_id:
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        + "_" + uuid.uuid4().hex[:8]
    )

GCS_PREFIX = f"clinical_source/{run_id}"

# ==========================================
# 2. TABLE NAMES AND ROW COUNTS
# ==========================================

names = [
    "clinical_studies",
    "clinical_sites",
    "investigators",
    "patients",
    "patient_enrollments",
    "patient_visits",
    "lab_results",
    "vital_signs",
    "adverse_events",
    "medications",
    "medical_history",
    "study_treatments"
]

counts = [
    1000, 1500, 2000, 7000,
    7000, 6000, 6500, 6000,
    3000, 4500, 4000, 5000
]

# ==========================================
# 3. TABLE SCHEMAS
# ==========================================

fields = [
    """study_id protocol_id study_title therapeutic_area
    study_phase study_type sponsor_name study_status
    start_date end_date planned_enrollment primary_endpoint
    study_country source_system""",

    """site_id study_id site_code site_name country city
    state postal_code site_address site_status
    activation_date site_type target_enrollment source_system""",

    """investigator_id site_id study_id first_name last_name
    email phone license_number specialty role hire_date
    training_status country source_system""",

    """patient_id site_id study_id first_name last_name
    date_of_birth sex_at_birth email phone address city
    country postal_code emergency_contact patient_status""",

    """enrollment_id patient_id study_id site_id screening_date
    enrollment_date consent_date eligibility_status
    randomization_group enrollment_status withdrawal_reason
    age_at_enrollment consent_version source_system""",

    """visit_id enrollment_id patient_id study_id site_id
    visit_number visit_type scheduled_date actual_date
    visit_status visit_window_days visit_notes
    performed_by source_system""",

    """lab_result_id visit_id patient_id study_id site_id
    test_name test_code result_value result_unit
    reference_low reference_high abnormal_flag sample_date
    lab_name source_system""",

    """vital_id visit_id patient_id study_id site_id
    measurement_date systolic_bp diastolic_bp heart_rate
    temperature_c respiratory_rate height_cm weight_kg bmi""",

    """ae_id enrollment_id patient_id study_id site_id
    event_term onset_date resolution_date severity
    serious_flag related_to_treatment outcome action_taken
    reported_date source_system""",

    """medication_id enrollment_id patient_id study_id site_id
    medication_name medication_code dose dose_unit frequency
    route start_date end_date indication""",

    """history_id patient_id study_id site_id condition_name
    condition_code diagnosis_date condition_status severity
    family_history_flag prior_hospitalization reported_by
    recorded_date notes source_system""",

    """treatment_id enrollment_id patient_id study_id site_id
    treatment_arm investigational_product dose_amount
    dose_unit administration_route start_date end_date
    treatment_status compliance_pct source_system"""
]

fields = [schema.split() for schema in fields]

# ==========================================
# 4. REFERENCE DATA
# ==========================================

areas = [
    "Oncology",
    "Neurology",
    "Cardiology",
    "Endocrinology"
]

conditions = [
    "Hypertension",
    "Diabetes",
    "Asthma",
    "Migraine",
    "Arthritis"
]

drugs = [
    "Metformin",
    "Lisinopril",
    "Atorvastatin",
    "Omeprazole"
]

def choice(values):
    return random.choice(values)

def dt():
    return f.date_between(
        start_date="-5y",
        end_date="today"
    ).isoformat()

def rid(prefix, index):
    return f"{prefix}_{index + 1:06}"

# ==========================================
# 5. GENERATE 12 CLINICAL TABLES
# ==========================================

total_rows = 0

for name, cols, count in zip(names, fields, counts):

    local_file = os.path.join(base, name + ".csv")

    with open(
        local_file,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=cols
        )

        writer.writeheader()

        for i in range(count):

            patient = i % 7000
            site = patient % 1500
            study = site % 1000
            visit = i % 6000

            context = {
                "study_id": rid("STUDY", study),
                "site_id": rid("SITE", site),
                "patient_id": rid("PAT", patient),
                "enrollment_id": rid("ENR", patient),
                "visit_id": rid("VIS", visit)
            }

            if name == "clinical_sites":
                context["study_id"] = rid(
                    "STUDY", i % 1000
                )

            if name == "investigators":
                context.update(
                    site_id=rid("SITE", i % 1500),
                    study_id=rid(
                        "STUDY", (i % 1500) % 1000
                    )
                )

            if name == "patient_visits":
                context.update(
                    patient_id=rid("PAT", i),
                    enrollment_id=rid("ENR", i),
                    site_id=rid("SITE", i % 1500),
                    study_id=rid(
                        "STUDY", (i % 1500) % 1000
                    )
                )

            if name in ("lab_results", "vital_signs"):
                context.update(
                    patient_id=rid("PAT", visit),
                    site_id=rid("SITE", visit % 1500),
                    study_id=rid(
                        "STUDY", (visit % 1500) % 1000
                    )
                )

            row = {}

            for col in cols:

                if col in context:
                    val = context[col]

                elif col.endswith("_id") or col in (
                    "protocol_id",
                    "site_code",
                    "license_number",
                    "test_code",
                    "medication_code",
                    "condition_code"
                ):
                    val = rid(col.upper(), i)

                elif col == "first_name":
                    val = f.first_name()

                elif col == "last_name":
                    val = f.last_name()

                elif col == "email":
                    val = (
                        f"fictional.{name}.{i + 1}@example.org"
                    )

                elif col == "phone":
                    val = f"202555{i % 10000:04d}"

                elif col in ("address", "site_address"):
                    val = f.street_address()

                elif col == "city":
                    val = f.city()

                elif col == "state":
                    val = f.state()

                elif col == "postal_code":
                    val = f.postcode()

                elif col == "emergency_contact":
                    val = f.name()

                elif col == "date_of_birth":
                    val = f.date_of_birth(
                        minimum_age=18,
                        maximum_age=85
                    ).isoformat()

                elif col.endswith("_date"):
                    val = dt()

                elif col in ("study_title", "site_name"):
                    val = f"Fictional {name} {i + 1}"

                elif col == "sponsor_name":
                    val = f"Fictional Biopharma {i % 30 + 1}"

                elif col in ("therapeutic_area", "specialty"):
                    val = choice(areas)

                elif col == "study_phase":
                    val = choice(["I", "II", "III", "IV"])

                elif col == "study_type":
                    val = choice([
                        "Interventional",
                        "Observational"
                    ])

                elif col == "primary_endpoint":
                    val = choice(["Safety", "Efficacy"])

                elif col in ("country", "study_country"):
                    val = choice([
                        "USA", "India", "UK", "Canada"
                    ])

                elif col.endswith("_status"):
                    val = choice([
                        "Active", "Completed", "Pending"
                    ])

                elif col == "source_system":
                    val = choice([
                        "EDC", "CTMS", "EHR", "LIMS"
                    ])

                elif col == "sex_at_birth":
                    val = choice(["Male", "Female"])

                elif col == "role":
                    val = choice([
                        "Principal Investigator",
                        "Sub-Investigator"
                    ])

                elif col == "site_type":
                    val = choice([
                        "Hospital",
                        "Clinic",
                        "Research Center"
                    ])

                elif col in (
                    "randomization_group",
                    "treatment_arm"
                ):
                    val = choice([
                        "Treatment A",
                        "Treatment B",
                        "Placebo"
                    ])

                elif col == "eligibility_status":
                    val = choice([
                        "Eligible",
                        "Screen Failure"
                    ])

                elif col == "consent_version":
                    val = choice(["v1", "v2"])

                elif col == "visit_type":
                    val = choice([
                        "Screening",
                        "Baseline",
                        "Follow-up"
                    ])

                elif col == "visit_notes":
                    val = "Routine protocol assessment"

                elif col == "performed_by":
                    val = rid("INV", i % 2000)

                elif col == "test_name":
                    val = choice([
                        "Glucose",
                        "Hemoglobin",
                        "Creatinine"
                    ])

                elif col == "result_unit":
                    val = "mg/dL"

                elif col.endswith("_flag"):
                    val = choice(["Y", "N"])

                elif col == "lab_name":
                    val = f"Fictional Lab {i % 25}"

                elif col == "event_term":
                    val = choice([
                        "Headache",
                        "Nausea",
                        "Fatigue"
                    ])

                elif col == "severity":
                    val = choice([
                        "Mild",
                        "Moderate",
                        "Severe"
                    ])

                elif col == "related_to_treatment":
                    val = choice([
                        "Related",
                        "Unrelated"
                    ])

                elif col == "outcome":
                    val = choice([
                        "Recovered",
                        "Ongoing"
                    ])

                elif col == "action_taken":
                    val = choice([
                        "None",
                        "Dose reduced"
                    ])

                elif col == "medication_name":
                    val = choice(drugs)

                elif col in ("condition_name", "indication"):
                    val = choice(conditions)

                elif col == "frequency":
                    val = choice([
                        "Daily",
                        "Twice daily"
                    ])

                elif col in ("route", "administration_route"):
                    val = "Oral"

                elif col == "dose_unit":
                    val = "mg"

                elif col == "reported_by":
                    val = choice([
                        "Patient",
                        "Clinician"
                    ])

                elif col == "notes":
                    val = "Synthetic medical history"

                elif col == "investigational_product":
                    val = choice([
                        "Compound A",
                        "Compound B",
                        "Placebo"
                    ])

                elif col == "withdrawal_reason":
                    val = choice([
                        "",
                        "Patient request"
                    ])

                elif col == "reference_low":
                    val = 70

                elif col == "reference_high":
                    val = 140

                elif col == "temperature_c":
                    val = round(
                        random.uniform(36, 38), 1
                    )

                elif col == "weight_kg":
                    val = round(
                        random.uniform(45, 110), 1
                    )

                elif col == "bmi":
                    val = round(
                        random.uniform(19, 33), 1
                    )

                elif col == "compliance_pct":
                    val = round(
                        random.uniform(70, 100), 1
                    )

                elif col == "result_value":
                    val = round(
                        random.uniform(65, 155), 2
                    )

                else:
                    val = random.randint(1, 120)

                row[col] = val

            writer.writerow(row)

    total_rows += count

    print(
        f"Generated: {name} | "
        f"Rows: {count} | Columns: {len(cols)}",
        flush=True
    )

# ==========================================
# 6. UPLOAD ALL CSV FILES TO GCS
# ==========================================

print("Connecting to Google Cloud Storage...", flush=True)

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

uploaded_files = 0

for name in names:

    local_file = os.path.join(
        base, name + ".csv"
    )

    if not os.path.exists(local_file):
        raise FileNotFoundError(
            f"Missing CSV file: {local_file}"
        )

    gcs_path = f"{GCS_PREFIX}/{name}.csv"

    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(
        local_file,
        content_type="text/csv"
    )

    uploaded_files += 1

    print(
        f"Uploaded: gs://{BUCKET_NAME}/{gcs_path}",
        flush=True
    )

# ==========================================
# 7. FINAL VALIDATION
# ==========================================

if uploaded_files != len(names):
    raise RuntimeError(
        f"Expected {len(names)} files, "
        f"uploaded {uploaded_files}"
    )

print("=" * 50)
print("CLINICAL DATA GENERATION COMPLETED")
print(f"Total Tables: {uploaded_files}")
print(f"Total Rows: {total_rows}")
print(f"GCS Bucket: {BUCKET_NAME}")
print(f"GCS Folder: {GCS_PREFIX}")
print("=" * 50)
