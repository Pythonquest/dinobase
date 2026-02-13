"""
PBDB Data Fetching Script

This script fetches data from the Paleobiology Database (PBDB) API
and loads it into BigQuery's pbdb_raw dataset.
"""

import os
import requests
import json
from typing import Dict, List, Optional
from google.cloud import bigquery
from google.oauth2 import service_account
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PBDB API base URL
PBDB_API_BASE = "https://paleobiodb.org/data1.2"

# BigQuery configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'dinobase-project')
DATASET_ID = 'pbdb_raw'
LOCATION = 'US'
CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')


class PBDBFetcher:
    """Fetches data from PBDB API and loads into BigQuery."""
    
    def __init__(self, project_id: str = PROJECT_ID, dataset_id: str = DATASET_ID, credentials_path: Optional[str] = None):
        """
        Initialize the fetcher with BigQuery client.
        
        Args:
            project_id: GCP project ID (if not provided and credentials file exists, will be extracted from credentials)
            dataset_id: BigQuery dataset ID
            credentials_path: Path to service account JSON key file (optional, uses GOOGLE_APPLICATION_CREDENTIALS env var if not provided)
        """
        self.dataset_id = dataset_id
        
        # Load credentials from service account file if provided
        credentials = None
        creds_path = credentials_path or CREDENTIALS_PATH
        if creds_path:
            if not os.path.exists(creds_path):
                raise FileNotFoundError(f"Credentials file not found: {creds_path}")
            logger.info(f"Loading credentials from: {creds_path}")
            
            # Load credentials and extract project_id from file if not explicitly provided
            with open(creds_path, 'r') as f:
                creds_data = json.load(f)
                if project_id == PROJECT_ID and 'project_id' in creds_data:
                    project_id = creds_data['project_id']
                    logger.info(f"Using project_id from credentials file: {project_id}")
            
            credentials = service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=['https://www.googleapis.com/auth/bigquery']
            )
        
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        self._ensure_dataset_exists()
    
    def _ensure_dataset_exists(self):
        """Ensure the pbdb_raw dataset exists in BigQuery."""
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = LOCATION
            dataset.description = "Raw, minimally transformed PBDB data"
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {self.dataset_id}")
    
    def fetch_occurrences(
        self,
        limit: int = 1000,
        offset: int = 0,
        **kwargs
    ) -> List[Dict]:
        """
        Fetch occurrence data from PBDB API.
        
        Args:
            limit: Maximum number of records to fetch
            offset: Offset for pagination
            **kwargs: Additional query parameters for PBDB API
        
        Returns:
            List of occurrence records
        """
        url = f"{PBDB_API_BASE}/occs/list.json"
        params = {
            'limit': limit,
            'offset': offset,
            **kwargs
        }
        
        logger.info(f"Fetching occurrences from PBDB API: {params}")
        response = requests.get(url, params=params)
        
        if not response.ok:
            error_msg = f"API request failed with status {response.status_code}"
            try:
                error_data = response.json()
                if 'warnings' in error_data:
                    logger.warning(f"API warnings: {error_data['warnings']}")
                if 'errors' in error_data:
                    logger.error(f"API errors: {error_data['errors']}")
                    error_msg += f": {error_data['errors']}"
            except:
                error_msg += f": {response.text[:500]}"
            raise requests.exceptions.HTTPError(error_msg, response=response)
        
        response.raise_for_status()
        
        data = response.json()
        records = data.get('records', [])
        logger.info(f"Fetched {len(records)} occurrence records")
        
        return records
    
    def load_to_bigquery(
        self,
        table_id: str,
        records: List[Dict],
        write_disposition: str = 'WRITE_APPEND'
    ):
        """
        Load records into BigQuery table.
        
        Args:
            table_id: Name of the BigQuery table
            records: List of records to load
            write_disposition: 'WRITE_APPEND', 'WRITE_TRUNCATE', or 'WRITE_EMPTY'
        """
        if not records:
            logger.warning(f"No records to load into {table_id}")
            return
        
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        
        # Load data
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
            autodetect=True
        )
        
        job = self.client.load_table_from_json(
            records,
            table_ref,
            job_config=job_config
        )
        
        job.result()  # Wait for the job to complete
        logger.info(f"Loaded {len(records)} records into {self.dataset_id}.{table_id}")
    
    def fetch_and_load_occurrences(
        self,
        table_id: str = 'occurrences',
        limit: int = 1000,
        offset: int = 0,
        write_disposition: str = 'WRITE_APPEND',
        **kwargs
    ):
        """
        Fetch occurrences from PBDB API and load into BigQuery.
        
        Args:
            table_id: BigQuery table name
            limit: Maximum records per API call
            offset: Offset for pagination
            write_disposition: BigQuery write disposition
            **kwargs: Additional PBDB API parameters
        """
        records = self.fetch_occurrences(limit=limit, offset=offset, **kwargs)
        self.load_to_bigquery(table_id, records, write_disposition)


def main():
    """Main entry point for the script."""
    fetcher = PBDBFetcher()
    
    # Example: Fetch and load occurrences
    # Adjust parameters as needed
    # Note: PBDB API requires at least one filter parameter - using 'all_records' to fetch all
    fetcher.fetch_and_load_occurrences(
        table_id='occurrences',
        limit=1000,
        offset=0,
        write_disposition='WRITE_APPEND',
        all_records=True
    )
    
    logger.info("Data fetch and load completed")


if __name__ == '__main__':
    main()
