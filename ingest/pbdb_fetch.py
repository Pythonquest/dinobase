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
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# PBDB API base URL
PBDB_API_BASE = "https://paleobiodb.org/data1.2"

# BigQuery configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dinobase-project")
DATASET_ID = "pbdb_raw"
LOCATION = "US"
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


class PBDBFetcher:
    """Fetches data from PBDB API and loads into BigQuery."""

    def __init__(
        self,
        project_id: str = PROJECT_ID,
        dataset_id: str = DATASET_ID,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize the fetcher with BigQuery client.

        Args:
            project_id: GCP project ID (falls back to credentials
                file if not provided)
            dataset_id: BigQuery dataset ID
            credentials_path: Path to service account JSON key file
                (uses GOOGLE_APPLICATION_CREDENTIALS env var as fallback)
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
            with open(creds_path, "r") as f:
                creds_data = json.load(f)
                if project_id == PROJECT_ID and "project_id" in creds_data:
                    project_id = creds_data["project_id"]
                    logger.info(f"Using project_id from credentials file: {project_id}")

            credentials = service_account.Credentials.from_service_account_file(
                creds_path, scopes=["https://www.googleapis.com/auth/bigquery"]
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

    def _fetch_from_api(
        self,
        endpoint: str,
        params: Dict,
    ) -> List[Dict]:
        """
        Fetch records from a PBDB API endpoint.

        Args:
            endpoint: API endpoint path (e.g. 'occs/list.json')
            params: Query parameters for the API call

        Returns:
            List of records from the API response
        """
        url = f"{PBDB_API_BASE}/{endpoint}"

        logger.info(f"Fetching from PBDB API {endpoint}: {params}")
        response = requests.get(url, params=params)

        if not response.ok:
            error_msg = f"API request failed with status {response.status_code}"
            try:
                error_data = response.json()
                if "warnings" in error_data:
                    logger.warning(f"API warnings: {error_data['warnings']}")
                if "errors" in error_data:
                    logger.error(f"API errors: {error_data['errors']}")
                    error_msg += f": {error_data['errors']}"
            except (ValueError, KeyError):
                error_msg += f": {response.text[:500]}"
            raise requests.exceptions.HTTPError(error_msg, response=response)

        data = response.json()
        records = data.get("records", [])
        logger.info(f"Fetched {len(records)} records from {endpoint}")

        return records

    def fetch_occurrences(
        self,
        limit: int = 1000,
        offset: int = 0,
        show: str = "coords,paleoloc",
        **kwargs,
    ) -> List[Dict]:
        """
        Fetch occurrence data from PBDB API with coordinates and paleocoordinates.

        Args:
            limit: Maximum number of records to fetch
            offset: Offset for pagination
            show: Comma-separated list of additional data blocks to include
            **kwargs: Additional query parameters for PBDB API

        Returns:
            List of occurrence records
        """
        params = {"limit": limit, "offset": offset, "show": show, **kwargs}
        return self._fetch_from_api("occs/list.json", params)

    def fetch_collections(
        self,
        limit: int = 1000,
        offset: int = 0,
        show: str = "coords,paleoloc,loc,strat,geo",
        **kwargs,
    ) -> List[Dict]:
        """
        Fetch collection data from PBDB API with full geographic and geological context.

        Args:
            limit: Maximum number of records to fetch
            offset: Offset for pagination
            show: Comma-separated list of additional data blocks to include
            **kwargs: Additional query parameters for PBDB API

        Returns:
            List of collection records
        """
        params = {"limit": limit, "offset": offset, "show": show, **kwargs}
        return self._fetch_from_api("colls/list.json", params)

    def fetch_taxa(
        self, limit: int = 1000, offset: int = 0, show: str = "app,classext", **kwargs
    ) -> List[Dict]:
        """
        Fetch taxonomic data from PBDB API with appearance times and classification hierarchy.

        Args:
            limit: Maximum number of records to fetch
            offset: Offset for pagination
            show: Comma-separated list of additional data blocks to include
            **kwargs: Additional query parameters for PBDB API

        Returns:
            List of taxon records
        """
        params = {"limit": limit, "offset": offset, "show": show, **kwargs}
        return self._fetch_from_api("taxa/list.json", params)

    def load_to_bigquery(
        self,
        table_id: str,
        records: List[Dict],
        write_disposition: str = "WRITE_APPEND",
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
            autodetect=True,
        )

        job = self.client.load_table_from_json(
            records, table_ref, job_config=job_config
        )

        job.result()  # Wait for the job to complete
        logger.info(f"Loaded {len(records)} records into {self.dataset_id}.{table_id}")

    def fetch_and_load_paginated(
        self,
        endpoint: str,
        table_id: str,
        page_size: int = 5000,
        extra_params: Optional[Dict] = None,
    ) -> int:
        """
        Page through a PBDB list endpoint and load all rows into BigQuery.

        The first page replaces the table; later pages append. Pass PBDB
        selection params (e.g. all_records, all_taxa) via extra_params.

        Returns:
            Total number of records loaded.
        """
        extra_params = dict(extra_params or {})
        offset = 0
        total = 0
        first_page = True
        while True:
            params = {"limit": page_size, "offset": offset, **extra_params}
            records = self._fetch_from_api(endpoint, params)
            if not records:
                break
            write_disposition = "WRITE_TRUNCATE" if first_page else "WRITE_APPEND"
            self.load_to_bigquery(
                table_id, records, write_disposition=write_disposition
            )
            total += len(records)
            first_page = False
            if len(records) < page_size:
                break
            offset += page_size
        logger.info(
            "Paginated load finished for %s -> %s (%s rows)",
            endpoint,
            table_id,
            total,
        )
        return total

    def fetch_taxonomic_opinions(
        self, limit: int = 5000, offset: int = 0, **kwargs
    ) -> List[Dict]:
        """Taxonomic opinions (taxa/opinions.json)."""
        params = {"limit": limit, "offset": offset, **kwargs}
        return self._fetch_from_api("taxa/opinions.json", params)

    def fetch_occurrence_opinions(
        self, limit: int = 5000, offset: int = 0, **kwargs
    ) -> List[Dict]:
        """Opinions tied to occurrence records (occs/opinions.json)."""
        params = {"limit": limit, "offset": offset, **kwargs}
        return self._fetch_from_api("occs/opinions.json", params)

    def fetch_references(
        self, limit: int = 5000, offset: int = 0, **kwargs
    ) -> List[Dict]:
        """Bibliographic references (refs/list.json); requires a selector such as all_records."""
        params = {"limit": limit, "offset": offset, **kwargs}
        return self._fetch_from_api("refs/list.json", params)

    def fetch_intervals(
        self, limit: int = 5000, offset: int = 0, **kwargs
    ) -> List[Dict]:
        """Geological time intervals (intervals/list.json)."""
        params = {"limit": limit, "offset": offset, **kwargs}
        return self._fetch_from_api("intervals/list.json", params)

    def fetch_timescales(
        self, limit: int = 5000, offset: int = 0, **kwargs
    ) -> List[Dict]:
        """Time scales (timescales/list.json)."""
        params = {"limit": limit, "offset": offset, **kwargs}
        return self._fetch_from_api("timescales/list.json", params)

    def fetch_and_load_occurrences(
        self,
        table_id: str = "occurrences",
        limit: int = 1000,
        offset: int = 0,
        write_disposition: str = "WRITE_APPEND",
        **kwargs,
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

    def fetch_and_load_collections(
        self,
        table_id: str = "collections",
        limit: int = 1000,
        offset: int = 0,
        write_disposition: str = "WRITE_APPEND",
        **kwargs,
    ):
        """
        Fetch collections from PBDB API and load into BigQuery.

        Args:
            table_id: BigQuery table name
            limit: Maximum records per API call
            offset: Offset for pagination
            write_disposition: BigQuery write disposition
            **kwargs: Additional PBDB API parameters
        """
        records = self.fetch_collections(limit=limit, offset=offset, **kwargs)
        self.load_to_bigquery(table_id, records, write_disposition)

    def fetch_and_load_taxa(
        self,
        table_id: str = "taxa",
        limit: int = 1000,
        offset: int = 0,
        write_disposition: str = "WRITE_APPEND",
        **kwargs,
    ):
        """
        Fetch taxa from PBDB API and load into BigQuery.

        Args:
            table_id: BigQuery table name
            limit: Maximum records per API call
            offset: Offset for pagination
            write_disposition: BigQuery write disposition
            **kwargs: Additional PBDB API parameters
        """
        records = self.fetch_taxa(limit=limit, offset=offset, **kwargs)
        self.load_to_bigquery(table_id, records, write_disposition)

    def fetch_and_load_taxonomic_opinions(
        self,
        table_id: str = "taxonomic_opinions",
        page_size: int = 5000,
        **kwargs,
    ) -> int:
        """Paginated load of taxa/opinions.json into BigQuery."""
        return self.fetch_and_load_paginated(
            "taxa/opinions.json", table_id, page_size=page_size, extra_params=kwargs
        )

    def fetch_and_load_occurrence_opinions(
        self,
        table_id: str = "occurrence_opinions",
        page_size: int = 5000,
        **kwargs,
    ) -> int:
        """Paginated load of occs/opinions.json into BigQuery."""
        return self.fetch_and_load_paginated(
            "occs/opinions.json", table_id, page_size=page_size, extra_params=kwargs
        )

    def fetch_and_load_references(
        self,
        table_id: str = "refs",
        page_size: int = 5000,
        **kwargs,
    ) -> int:
        """Paginated load of refs/list.json into BigQuery."""
        return self.fetch_and_load_paginated(
            "refs/list.json", table_id, page_size=page_size, extra_params=kwargs
        )

    def fetch_and_load_intervals(
        self,
        table_id: str = "intervals",
        page_size: int = 5000,
        **kwargs,
    ) -> int:
        """Paginated load of intervals/list.json into BigQuery."""
        return self.fetch_and_load_paginated(
            "intervals/list.json", table_id, page_size=page_size, extra_params=kwargs
        )

    def fetch_and_load_timescales(
        self,
        table_id: str = "timescales",
        page_size: int = 5000,
        **kwargs,
    ) -> int:
        """Load timescales/list.json (typically one short paginated pass)."""
        return self.fetch_and_load_paginated(
            "timescales/list.json", table_id, page_size=page_size, extra_params=kwargs
        )


def main():
    """Main entry point for the script."""
    fetcher = PBDBFetcher()

    fetcher.fetch_and_load_occurrences(
        table_id="occurrences",
        limit=1000,
        offset=0,
        write_disposition="WRITE_TRUNCATE",
        all_records=True,
    )

    fetcher.fetch_and_load_collections(
        table_id="collections",
        limit=1000,
        offset=0,
        write_disposition="WRITE_TRUNCATE",
        all_records=True,
    )

    fetcher.fetch_and_load_taxa(
        table_id="taxa",
        limit=1000,
        offset=0,
        write_disposition="WRITE_TRUNCATE",
        all_records=True,
    )

    # Classification opinions only (op_type=class) keeps volume manageable; use op_type="all" if needed.
    fetcher.fetch_and_load_taxonomic_opinions(
        table_id="taxonomic_opinions",
        page_size=5000,
        all_taxa=True,
        op_type="class",
    )
    fetcher.fetch_and_load_occurrence_opinions(
        table_id="occurrence_opinions",
        page_size=5000,
        all_records=True,
        op_type="class",
    )
    fetcher.fetch_and_load_references(
        table_id="refs",
        page_size=5000,
        all_records=True,
    )
    fetcher.fetch_and_load_intervals(
        table_id="intervals",
        page_size=5000,
        all_records=True,
    )
    fetcher.fetch_and_load_timescales(
        table_id="timescales",
        page_size=5000,
        all_records=True,
    )

    logger.info("Data fetch and load completed")


if __name__ == "__main__":
    main()
