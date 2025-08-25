FROM apache/airflow:3.0.4-python3.11

# Install curl (required for Poetry install)
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Poetry (official recommended method)

# Install latest Poetry (compatible with poetry-plugin-export)
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry


# Install poetry-plugin-export
RUN poetry self add poetry-plugin-export

# Copy dependency files
COPY pyproject.toml /tmp/pyproject.toml
COPY poetry.lock /tmp/poetry.lock

WORKDIR /tmp
# Export requirements as root
RUN poetry export -f requirements.txt --output /tmp/requirements.txt --without-hashes
# Install as airflow user
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt
USER root
RUN rm -rf /tmp/pyproject.toml /tmp/poetry.lock /tmp/requirements.txt
WORKDIR /opt/airflow

# (Optional) Copy plugins, dags, config, etc. if you want to bake them in
# COPY dags /opt/airflow/dags
# COPY plugins /opt/airflow/plugins
# COPY config /opt/airflow/config
USER airflow
