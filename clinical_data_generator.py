import pandas as pd
import random
from faker import Faker
import numpy as np

fake = Faker()

# Number of original records
num_records = 5000

data = []

for i in range(num_records):

    patient_id = f"PAT{100001 + i}"

    study_id = random.choice([
        "STUDY_101",
        "STUDY_102",
        "STUDY_103",
        "STUDY_104",
        "STUDY_105"
    ])

    gender = random.choice(["Male", "Female"])

    age = random.randint(18, 85)

    country = random.choice([
        "India",
        "USA",
        "UK",
        "Germany",
        "Canada"
    ])

    condition = random.choice([
        "Diabetes",
        "Hypertension",
        "Asthma",
        "Arthritis",
        "Migraine"
    ])

    treatment = random.choice([
        "Drug_A",
        "Drug_B",
        "Drug_C",
        "Placebo"
    ])

    visit_date = fake.date_between(
        start_date="-2y",
        end_date="today"
    )

    trial_phase = random.choice([
        "Phase I",
        "Phase II",
        "Phase III",
        "Phase IV"
    ])

    status = random.choice([
        "Active",
        "Completed",
        "Withdrawn",
        "Screening"
    ])

    adverse_event = random.choice([
        "None",
        "Headache",
        "Nausea",
        "Fatigue",
        "Dizziness"
    ])

    data.append([
        patient_id,
        study_id,
        gender,
        age,
        country,
        condition,
        treatment,
        visit_date,
        trial_phase,
        status,
        adverse_event
    ])


columns = [
    "patient_id",
    "study_id",
    "gender",
    "age",
    "country",
    "medical_condition",
    "treatment",
    "visit_date",
    "trial_phase",
    "study_status",
    "adverse_event"
]


# Create DataFrame
df = pd.DataFrame(data, columns=columns)


# ==========================================================
# ADD RANDOM DIRTY DATA
# ==========================================================


# 1. RANDOM DUPLICATES
# Randomly creates between 20 and 100 duplicate records

num_duplicates = random.randint(20, 100)

duplicate_rows = df.sample(
    n=num_duplicates,
    replace=False
)

df = pd.concat(
    [df, duplicate_rows],
    ignore_index=True
)


# 2. RANDOM NULL / NA VALUES

null_columns = [
    "gender",
    "age",
    "country",
    "medical_condition",
    "treatment",
    "adverse_event"
]

for column in null_columns:

    num_nulls = random.randint(5, 20)

    indexes = df.sample(
        n=num_nulls
    ).index

    df.loc[indexes, column] = np.nan


# 3. RANDOM BLANK VALUES

blank_columns = [
    "country",
    "medical_condition",
    "treatment",
    "adverse_event"
]

for column in blank_columns:

    num_blanks = random.randint(5, 15)

    indexes = df.sample(
        n=num_blanks
    ).index

    df.loc[indexes, column] = ""


# 4. RANDOM WHITESPACE VALUES

indexes = df.sample(
    n=random.randint(5, 10)
).index

df.loc[indexes, "country"] = "   "


# 5. INVALID AGE - NEGATIVE

indexes = df.sample(
    n=random.randint(3, 8)
).index

df.loc[indexes, "age"] = -5


# 6. INVALID AGE - TOO HIGH

indexes = df.sample(
    n=random.randint(3, 8)
).index

df.loc[indexes, "age"] = 150


# 7. INCONSISTENT GENDER

indexes = df.sample(
    n=random.randint(3, 8)
).index

df.loc[indexes, "gender"] = "male"


indexes = df.sample(
    n=random.randint(3, 8)
).index

df.loc[indexes, "gender"] = "FEMALE"


# 8. COUNTRY CASE ISSUE

indexes = df.sample(
    n=random.randint(3, 8)
).index

df.loc[indexes, "country"] = "india"


# 9. COUNTRY EXTRA SPACE ISSUE

indexes = df.sample(
    n=random.randint(3, 8)
).index

df.loc[indexes, "country"] = "USA "


# 10. INVALID STUDY IDs

indexes = df.sample(
    n=random.randint(3, 8)
).index

df.loc[indexes, "study_id"] = "INVALID_STUDY"


# 11. MISSING PATIENT IDs

indexes = df.sample(
    n=random.randint(2, 5)
).index

df.loc[indexes, "patient_id"] = ""


# 12. INVALID VISIT DATES

indexes = df.sample(
    n=random.randint(3, 8)
).index

df.loc[indexes, "visit_date"] = "INVALID_DATE"


# 13. SHUFFLE EVERYTHING
# So duplicates and bad records are not grouped together

df = df.sample(
    frac=1
).reset_index(drop=True)


# ==========================================================
# SAVE CSV
# ==========================================================

df.to_csv(
    r"C:\Users\Admin\Desktop\GCP\clinical_data.csv",
    index=False
)


# ==========================================================
# PRINT RESULTS
# ==========================================================

print("Clinical CSV file created successfully!")

print("Original records:", num_records)

print("Random duplicates added:", num_duplicates)

print("Final records:", len(df))

print("Exact duplicates detected:", df.duplicated().sum())


print("\nNull values:")
print(df.isnull().sum())


print("\nSample Data:")
print(df.head(10))