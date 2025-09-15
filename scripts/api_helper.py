import os
from typing import Any, Dict, List, cast

import requests
from dotenv import load_dotenv

"""
API helper functions for interacting with the data ingestor API.
"""

load_dotenv()
endpoint = os.getenv("INGESTOR_API")


def list_sources() -> List[str]:
    """List all sources."""
    response = requests.get(f"{endpoint}/sources")
    response.raise_for_status()
    data = response.json()
    sources = data.get("sources")
    if not isinstance(sources, list) or not all(isinstance(s, str) for s in sources):
        raise ValueError("API did not return a valid list of sources")
    return sources

def get_source_metadata(source_name: str) -> Dict[str, Any]:
    """Get metadata for a specific source."""
    response = requests.get(f"{endpoint}/sources/{source_name}")
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("API did not return a valid metadata dictionary")
    return cast(Dict[str, Any], data)

def ingest_sources(sources: List[str]) -> Dict[str, Any]:
    """Ingest a list of sources."""
    response = requests.post(f"{endpoint}/run", json={"sources": sources})
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("API did not return a valid ingestion result dictionary")
    return cast(Dict[str, Any], data)
