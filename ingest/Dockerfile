# Dockerfile for PBDB data ingestion
# Optional - use if you want to containerize the ingestion process

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY pbdb_fetch.py .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the ingestion script
CMD ["python", "pbdb_fetch.py"]
