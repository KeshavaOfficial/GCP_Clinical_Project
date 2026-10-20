
from google.cloud import bigquery

def read_pipeline_config():
    client = bigquery.Client(project="vaarahigcpproject")

    query = """
        SELECT *
        FROM `vaarahigcpproject.clinical_control.pipeline_config`
        WHERE pipeline_id = 'CLINICAL_API_001'
          AND is_active = TRUE
    """

    rows = list(client.query(query, location="US").result())

    if len(rows) != 1:
        raise ValueError("Expected exactly one active pipeline configuration")

    config = dict(rows[0].items())

    print("Pipeline ID:", config["pipeline_id"])
    print("Bronze Table:", config["bronze_table"])
    print("Silver Table:", config["silver_table"])

    return config
