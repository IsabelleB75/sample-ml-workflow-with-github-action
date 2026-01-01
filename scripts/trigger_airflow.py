import requests
import os
import sys
import json
from datetime import datetime, timezone

# ------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------
AIRFLOW_URL = os.getenv("AIRFLOW_URL")  # ex: http://78.47.129.250:8080
USERNAME = os.getenv("AIRFLOW_USER")
PASSWORD = os.getenv("AIRFLOW_PASS")
DAG_ID = os.getenv("DAG_ID", "ml_training_pipeline")  # Notre DAG
DOCKER_USERNAME = os.getenv("DOCKER_USERNAME", "isa752025")
GIT_HASH = sys.argv[1] if len(sys.argv) > 1 else "latest"

if not AIRFLOW_URL:
    print("❌ Error: AIRFLOW_URL env var is missing")
    sys.exit(1)

HEADERS = {
    "Content-Type": "application/json",
}

def trigger_dag():
    """
    Trigger le DAG Airflow via l'API REST (Basic Auth pour Airflow 2.x)
    """
    # Airflow 2.x utilise /api/v1/
    trigger_url = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    
    print(f"🚀 Triggering DAG: {DAG_ID}")
    print(f"   URL: {trigger_url}")
    print(f"   Commit SHA: {GIT_HASH}")
    
    # Configuration passée au DAG
    docker_image = f"{DOCKER_USERNAME}/sample-ml-workflow:{GIT_HASH}"
    
    payload = {
        "conf": {
            "commit_sha": GIT_HASH,
            "docker_image": docker_image
        }
    }
    
    try:
        # Airflow 2.x utilise Basic Auth directement
        response = requests.post(
            trigger_url, 
            json=payload, 
            headers=HEADERS,
            auth=(USERNAME, PASSWORD),
            timeout=30
        )
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ Success! DAG Run ID: {result.get('dag_run_id')}")
        print(f"   State: {result.get('state')}")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Trigger Failed: {e}")
        if 'response' in locals():
            print(f"   Response: {response.text}")
        sys.exit(1)

if __name__ == "__main__":
    trigger_dag()
