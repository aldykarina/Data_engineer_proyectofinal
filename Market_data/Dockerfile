FROM apache/airflow:2.9.2
COPY requirements.txt /

# Actualiza pip
RUN pip install --upgrade pip

# Instala dependencias del sistema
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instala pandas antes de Apache Airflow
RUN pip install pandas

# Instala Apache Airflow y las dependencias del requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt