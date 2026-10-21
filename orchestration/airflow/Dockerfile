# Airflow 3 for the local nightly demo, plus a separate venv with the project's own requirements
# (DuckDB, dbt Core, dbt-duckdb, and gspread for the BI export), so dbt and Airflow dependencies never meet.
FROM apache/airflow:3.3.2-python3.12

USER root
COPY --from=project requirements.txt /tmp/project-requirements.txt
COPY --from=project requirements-bi.txt /tmp/project-requirements-bi.txt
RUN python -m venv /opt/project-venv \
 && /opt/project-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/project-venv/bin/pip install --no-cache-dir -r /tmp/project-requirements.txt \
    -r /tmp/project-requirements-bi.txt \
 && rm /tmp/project-requirements.txt /tmp/project-requirements-bi.txt
COPY start.sh /opt/start.sh
RUN chmod 0755 /opt/start.sh

USER airflow
# pytest for the DAG tests, which run inside the Airflow environment
RUN pip install --no-cache-dir pytest==9.1.1
