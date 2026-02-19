# GCP Project ID Setup Guide

This guide shows you exactly where to update your GCP project ID in the repository.

## Step 1: Find Your GCP Project ID

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project (or create a new one)
3. Your Project ID is shown in the project selector at the top of the page
   - Example: `my-pbdb-project-123456`

## Step 2: Update Files

### Option A: Set Environment Variable (Recommended)

Set the `GCP_PROJECT_ID` environment variable, and most files will use it automatically:

**Windows PowerShell:**
```powershell
$env:GCP_PROJECT_ID = "your-actual-project-id"
```

**Windows Command Prompt:**
```cmd
set GCP_PROJECT_ID=your-actual-project-id
```

**Linux/Mac:**
```bash
export GCP_PROJECT_ID=your-actual-project-id
```

**Make it permanent:**
- Windows: Add to System Environment Variables
- Linux/Mac: Add to `~/.bashrc` or `~/.zshrc`

### Option B: Update Files Manually

If you prefer to hardcode the project ID, update these files:

#### 1. `dbt/pbdb_dbt/dbt_project.yml`

**Line 34:** Update the `project_id` variable:
```yaml
vars:
  project_id: "your-actual-project-id"  # Replace your-gcp-project-id
```

#### 2. `dbt/pbdb_dbt/profiles.yml.example`

**Lines 10 and 21:** Update the fallback project ID:
```yaml
project: "{{ env_var('GCP_PROJECT_ID', 'your-actual-project-id') }}"
```

Then copy this file to your dbt profiles location:
```bash
# Windows
copy dbt\pbdb_dbt\profiles.yml.example %USERPROFILE%\.dbt\profiles.yml

# Linux/Mac
cp dbt/pbdb_dbt/profiles.yml.example ~/.dbt/profiles.yml
```

#### 3. `ingest/pbdb_fetch.py`

**Line 27:** Update the default project ID:
```python
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'your-actual-project-id')
```

## Step 3: Verify Your Setup

### Test dbt Connection
```bash
cd dbt/pbdb_dbt
dbt debug
```

### Test Python Script
```bash
cd ingest
python -c "import os; print(os.getenv('GCP_PROJECT_ID', 'Not set'))"
```

## Step 4: Create BigQuery Datasets

After setting your project ID, create the datasets:

```bash
# Using environment variable
bq mk --dataset --location=US ${GCP_PROJECT_ID}:pbdb_raw
bq mk --dataset --location=US ${GCP_PROJECT_ID}:pbdb_analytics

# Or hardcode it
bq mk --dataset --location=US your-actual-project-id:pbdb_raw
bq mk --dataset --location=US your-actual-project-id:pbdb_analytics
```

## Step 5: GitHub Actions (Optional)

If using GitHub Actions CI/CD, add these secrets:

1. Go to your GitHub repository
2. Settings → Secrets and variables → Actions
3. Click "New repository secret" and add each of the following:

| Secret | Value | Required for |
|--------|-------|--------------|
| `GCP_PROJECT_ID` | Your GCP project ID (e.g. `dinobase-project-487317`) | All dbt CI jobs |
| `GCP_SA_KEY` | Full contents of a service account JSON key file | `test-dbt` job only |

**Creating the service account key for `GCP_SA_KEY`:**

1. Go to [GCP Console](https://console.cloud.google.com/) → IAM & Admin → Service Accounts
2. Create a service account (or use an existing one) with **BigQuery User** and **BigQuery Data Editor** roles
3. Go to the Keys tab → Add Key → Create new key → JSON
4. Paste the entire downloaded JSON file contents as the secret value

## Quick Reference: All Locations

| File | Line | What to Change |
|------|------|----------------|
| `dbt/pbdb_dbt/dbt_project.yml` | 34 | `project_id: "your-actual-project-id"` |
| `dbt/pbdb_dbt/profiles.yml.example` | 10, 21 | Fallback in `env_var()` |
| `ingest/pbdb_fetch.py` | 27 | Default in `os.getenv()` |
| `~/.dbt/profiles.yml` | 10, 21 | After copying from example |
| GitHub Secrets | - | `GCP_PROJECT_ID` and `GCP_SA_KEY` secrets |

## Notes

- **Best Practice**: Use environment variables (`GCP_PROJECT_ID`) rather than hardcoding
- The `profiles.yml.example` file uses environment variables with fallbacks
- The ingestion script checks environment variables first, then falls back to hardcoded value
- dbt models use the `project_id` variable from `dbt_project.yml`
