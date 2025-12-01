# Base: Official Apache Airflow image
FROM apache/airflow:2.9.1-python3.10

# Define build-time variables for Beam environment
ARG BEAM_VENV_PATH=/opt/airflow/beam_env
ARG REQUIREMENTS_BEAM=requirements-beam.txt

# Make variables available at runtime
ENV BEAM_VENV_PATH=${BEAM_VENV_PATH}
ENV REQUIREMENTS_BEAM=${REQUIREMENTS_BEAM}

# Switch to root to install system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy Airflow requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Optional: copy custom Python site config if needed
# COPY sitecustomize.py /usr/local/lib/python3.10/site-packages/

# Copy Beam-specific requirements
COPY ${REQUIREMENTS_BEAM} /opt/airflow/

# Create isolated venv for Beam
RUN python3 -m venv ${BEAM_VENV_PATH} \
 && . ${BEAM_VENV_PATH}/bin/activate \
 && pip install --upgrade pip \
 && pip install -r /opt/airflow/${REQUIREMENTS_BEAM}
