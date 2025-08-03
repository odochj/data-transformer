import requests
from dotenv import load_dotenv
import os

"""
API helper functions for interacting with the data ingestor API.
"""

load_dotenv()
endpoint = os.getenv("INGESTOR_API")

def list_sources():
    """List all sources."""
    response = requests.get(f"{endpoint}/sources")
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def get_source_metadata(source_name):
    """Get metadata for a specific source."""
    response = requests.get(f"{endpoint}/sources/{source_name}")
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

#TODO: 'Unprocessable content' error. 
def ingest_sources(sources: list):
    """Ingest a list of sources."""
    response = requests.post(f"{endpoint}/run", json={"sources": sources})
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()
