FROM apache/airflow:2.6.0-python3.9

# Install system dependencies for psycopg2 (optional if using psycopg2-binary)
USER root
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --user -r /opt/airflow/requirements.txt