
import requests
import google.auth.transport.requests
import google.oauth2.id_token


CLOUD_RUN_URL = (
    "https://clinical-flask-api-376396091900."
    "us-central1.run.app"
)


def trigger_flask_api():

    auth_request = google.auth.transport.requests.Request()

    token = google.oauth2.id_token.fetch_id_token(
        auth_request,
        CLOUD_RUN_URL
    )

    response = requests.post(
        f"{CLOUD_RUN_URL}/generate",
        headers={"Authorization": f"Bearer {token}"},
        timeout=300
    )

    response.raise_for_status()

    result = response.json()

    if result.get("status") != "success":
        raise ValueError("Flask API generation failed")

    print("Records generated:", result["total_records"])
    print("GCS file:", result["file"])

    return result["file"]
