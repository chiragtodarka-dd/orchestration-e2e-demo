# dcp-e2e-demo

source venv_py312/bin/activate && pip install -r orchestration/function/requirements.txt

source venv_py312/bin/activate && PYTHONPATH=$(pwd) python airflow/interface/function/function_to_operator_generator.py /Users/chirag.todarka/Projects/dcp-e2e-demo/orchestration/function/snowflake_sql_function.py /Users/chirag.todarka/Projects/dcp-e2e-demo/airflow/operator


airflow variables set postgres_host your_host
airflow variables set postgres_port your_port
airflow variables set postgres_db your_database
airflow variables set postgres_user your_user
airflow variables set postgres_password your_password

CREATE TABLE source (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE sink (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    processed_at TIMESTAMP
);

req.txt
where to put or vs airflow
dynamic dag generation

Sample Uasge `python airflow/interface/function/function_to_operator_generator.py orchestration/function/snowflake_sql_function.py airflow/operator` <br>


`source venv_py312/bin/activate && PYTHONPATH=$(pwd) python airflow/interface/function/function_to_operator_generator.py $(pwd)/orchestration/function/postgres_sql_function.py $(pwd)/airflow/operator`

docker compose run airflow-webserver airflow standalone

docker compose run airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin

docker compose up -d



docker compose down --remove-orphans && docker compose up -d && sleep 1 && docker compose exec postgres psql -U airflow -d airflow -c "\dt" && docker compose logs -f airflow-standalone

You should see the print statements from job_parser.py indicating the paths it's using and the DAGs it's loading.
Look for lines like "JobParser Initialized:", "Scanning for YAML DAGs in: /opt/airflow/dags", and "Successfully loaded DAG: ...".
Check the Airflow UI at http://localhost:8080 to see if your YAML-defined DAGs appear.


sleep 15 && docker compose exec postgres psql -U airflow -d airflow -c "\dt"