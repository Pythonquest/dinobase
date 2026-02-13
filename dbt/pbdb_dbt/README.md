# PBDB dbt Project

This dbt project transforms raw PBDB (Paleobiology Database) data from the `pbdb_raw` dataset into analytics-ready models.

## Project Structure

```
pbdb_dbt/
├── dbt_project.yml          # Project configuration
├── models/
│   ├── _sources.yml         # Source definitions for pbdb_raw dataset
│   ├── schema.yml           # Schema documentation for marts models
│   └── staging/
│       ├── _staging__models.yml  # Staging model documentation
│       └── stg_occurrences.sql   # Staging model for occurrences
└── README.md
```

## Setup

### 1. Authentication

The project is configured to use OAuth authentication for local development. To authenticate:

```bash
# Option 1: Use gcloud CLI (recommended)
gcloud auth application-default login

# Option 2: Use service account (for production)
# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable to your service account key file
```

### 2. Verify Connection

```bash
cd dbt/pbdb_dbt
dbt debug
```

This will verify your dbt configuration and test the BigQuery connection.

### 3. Run Models

```bash
# Compile models (check SQL without running)
dbt compile

# Run all models
dbt run

# Run specific model
dbt run --select stg_occurrences

# Run models and tests
dbt build
```

## Data Sources

### pbdb_raw Dataset

The project references the `pbdb_raw` dataset in BigQuery, which contains raw data loaded from the PBDB API.

**Source Tables:**
- `occurrences` - Raw occurrence data from PBDB API

## Models

### Staging Models

Staging models (`stg_*`) clean and standardize raw data from `pbdb_raw`:

- **stg_occurrences**: Cleans and standardizes occurrence data
  - Renames columns for clarity (e.g., `lng` → `longitude`, `lat` → `latitude`)
  - Calculates `midpoint_ma` (midpoint age) from `max_ma` and `min_ma`
  - Adds `_dbt_loaded_at` timestamp for tracking

### Marts Models

Marts models (to be created) will combine staging models into business-ready datasets.

## Configuration

### Profiles

Profiles are configured in `~/.dbt/profiles.yml`:

- **dev**: Uses OAuth authentication for local development
- **prod**: Uses service account authentication (requires `GOOGLE_APPLICATION_CREDENTIALS` env var)

### Project Variables

- `project_id`: GCP project ID (dinobase-project)

## Next Steps

1. **Authenticate with BigQuery** using one of the methods above
2. **Run `dbt debug`** to verify connection
3. **Run `dbt run`** to create staging models
4. **Create marts models** as needed for your analytics use cases
5. **Add tests** to ensure data quality

## Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt BigQuery Adapter](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile)
- [PBDB API Documentation](https://paleobiodb.org/data1.2/)
