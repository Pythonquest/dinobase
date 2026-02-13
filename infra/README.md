# Infrastructure

This directory contains infrastructure as code for the PBDB pipeline.

## Terraform

The `terraform/` directory contains Terraform configurations for provisioning GCP resources such as:
- BigQuery datasets
- Service accounts
- IAM roles and permissions
- Cloud Storage buckets (if needed)
- Cloud Scheduler jobs (for scheduled ingestion)

## Setup

1. Install Terraform: https://www.terraform.io/downloads
2. Authenticate with GCP: `gcloud auth application-default login`
3. Initialize Terraform: `terraform init`
4. Plan changes: `terraform plan`
5. Apply changes: `terraform apply`
