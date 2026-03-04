FROM apache/airflow:2.10.4-python3.11

USER root

# Install system dependencies for psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files into the Airflow home
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root etl/  /opt/airflow/etl/
COPY --chown=airflow:root sql/  /opt/airflow/sql/
COPY --chown=airflow:root raw_logs.json /opt/airflow/data/raw_logs.json
