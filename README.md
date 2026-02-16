# PBDB Pipeline

A data pipeline for fetching, loading, and transforming Paleobiology Database (PBDB) data using BigQuery and dbt.

## Architecture

![Dinobase Pipeline Architecture](docs/architecture_diagram.png)

This pipeline extracts data from the Paleobiology Database (PBDB),
lands raw JSON in Google Cloud Storage, loads into BigQuery,
and transforms using dbt Core into analytics-ready models.
Designed with modular ingestion and scalable transformation patterns.


## Repository Structure

```
pbdb-pipeline/
├── ingest/                    # Data ingestion scripts
│   ├── pbdb_fetch.py         # Main script to fetch PBDB data and load to BigQuery
│   ├── requirements.txt      # Python dependencies for ingestion
│   └── Dockerfile            # Optional Docker container for ingestion
├── infra/                    # Infrastructure as code
│   └── terraform/            # Optional Terraform configurations
├── dbt/                      # dbt project
│   └── pbdb_dbt/
│       ├── dbt_project.yml   # dbt project configuration
│       ├── profiles.yml.example  # Example profiles (copy to ~/.dbt/)
│       ├── models/
│       │   ├── staging/      # Staging models (views)
│       │   ├── marts/        # Mart models (tables)
│       │   ├── _sources.yml  # Source definitions for pbdb_raw
│       │   └── schema.yml   # Model documentation
│       ├── macros/           # dbt macros
│       └── tests/            # Custom dbt tests
├── .github/
│   └── workflows/
│       └── ci.yml            # GitHub Actions CI workflow
└── README.md                 # This file
```

## Dataset Structure

### `pbdb_raw`
- **Purpose**: Raw, minimally transformed data loaded into BigQuery
- **Location**: BigQuery dataset `pbdb_raw`
- **Content**: Source data with minimal transformations (e.g., type casting, basic cleaning)
- **Populated by**: `ingest/pbdb_fetch.py`
- **Access**: Read-only for dbt models (via sources)

### `pbdb_analytics`
- **Purpose**: Transformed data models built with dbt
- **Location**: BigQuery dataset `pbdb_analytics`
- **Content**: dbt models, views, and materialized tables
- **Structure**:
  - `staging/`: Staging models that clean and standardize raw data (views)
  - `marts/`: Business logic and analytics models (tables)

## Setup

### Prerequisites

- Python 3.11+
- Google Cloud SDK (`gcloud`)
- dbt CLI (`pip install dbt-bigquery`)
- BigQuery API enabled in your GCP project

### 1. Configure GCP Project

Set your GCP project ID:
```bash
export GCP_PROJECT_ID=your-gcp-project-id
```

Or update it in:
- `dbt/pbdb_dbt/dbt_project.yml` (vars section)
- `ingest/pbdb_fetch.py` (PROJECT_ID constant)

### 2. Create BigQuery Datasets

**Option A: Using bq CLI** (recommended):
```bash
bq mk --dataset --location=US ${GCP_PROJECT_ID}:pbdb_raw
bq mk --dataset --location=US ${GCP_PROJECT_ID}:pbdb_analytics
```

**Option B: Via BigQuery Console**:
- Navigate to BigQuery
- Create dataset `pbdb_raw`
- Create dataset `pbdb_analytics`

### 3. Set Up dbt

1. **Install dbt-bigquery**:
   ```bash
   pip install dbt-bigquery
   ```

2. **Configure profiles**:
   ```bash
   # Copy the example profile
   cp dbt/pbdb_dbt/profiles.yml.example ~/.dbt/profiles.yml
   # On Windows: copy dbt\pbdb_dbt\profiles.yml.example %USERPROFILE%\.dbt\profiles.yml
   
   # Edit ~/.dbt/profiles.yml with your GCP project ID
   ```

3. **Authenticate**:
   ```bash
   gcloud auth application-default login
   ```

4. **Test connection**:
   ```bash
   cd dbt/pbdb_dbt
   dbt debug
   ```

### 4. Set Up Data Ingestion

1. **Install Python dependencies**:
   ```bash
   cd ingest
   pip install -r requirements.txt
   ```

2. **Authenticate** (if not already done):
   ```bash
   gcloud auth application-default login
   ```

3. **Run ingestion**:
   ```bash
   python pbdb_fetch.py
   ```

## Usage

### Data Ingestion

```bash
cd ingest
python pbdb_fetch.py
```

The script will:
- Fetch data from PBDB API
- Load it into BigQuery `pbdb_raw` dataset
- Handle pagination and error handling

### dbt Models

```bash
cd dbt/pbdb_dbt

# Run all models
dbt run

# Run only staging models
dbt run --select staging.*

# Run only marts
dbt run --select marts.*

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Full refresh (recreate tables)
dbt run --full-refresh
```

## CI/CD

The project includes a GitHub Actions workflow (`.github/workflows/ci.yml`) that:
- Lints dbt code
- Lints Python code
- Runs dbt tests (if GCP credentials are configured)

To enable CI/CD:
1. Add GitHub secrets:
   - `GCP_PROJECT_ID`: Your GCP project ID
   - `GCP_SA_KEY`: Service account key JSON (for running tests)

## Development

### Adding New Models

1. **Staging models**: Add SQL files to `dbt/pbdb_dbt/models/staging/`
2. **Mart models**: Add SQL files to `dbt/pbdb_dbt/models/marts/`
3. **Documentation**: Update `dbt/pbdb_dbt/models/schema.yml`

### Adding New Sources

Update `dbt/pbdb_dbt/models/_sources.yml` when new tables are added to `pbdb_raw`.

### Adding Tests

- Generic tests: Add to `schema.yml` or `_sources.yml`
- Custom tests: Add SQL files to `dbt/pbdb_dbt/tests/`

## Notes

- `profiles.yml` should **NOT** be committed to version control (use `profiles.yml.example`)
- Update `your-gcp-project-id` placeholders with your actual GCP project ID
- The ingestion script uses the PBDB API v1.2
- dbt models use views for staging (fast, always fresh) and tables for marts (performance)
