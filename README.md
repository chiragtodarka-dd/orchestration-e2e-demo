#### Philosophy
   - Clear separation between business logic and technical implementation
   - A workflow is a collection of inter-dependent tasks, a workflow is generically refered as job in technology agnostic terminology
   - Client/end user 
      - Provide implementation of custom logic as 'functions' that can be run via orchestrator. Function are the building block on which task are build
      - Define the workflow as job definition and provide required business logic (sql)
   - Orchestartion team 
      - Responsible to own and maintain the orchestration infrastructure
      - Will select appropriate technology (such as from Airflow/Dagster/other) as per the business needs
      - Will provide and own the logic to translate technology agnostic artifacts to technology sepcific artifacts
   - If/when orchestartion team see the need of changing underlying technology such as from Airflow to Dagster or any other combination, it will provide and own the logic to convert technology agnostic artifacts to new technology sepcific artifacts
   - Based on this philosophy and design changing the underlying orchestartion technology will not be exposed to users and there will be no effort at end users side

#### Note
- For this demo using Airflow as choice for orchestartion but in production it can be any other orchestartion too

#### Key Components and Project Structure:
1. `orchestration/`
   Contains orchestration technology agnostic artifacts
   Clients/end users will be primary contributers in this folder
   This folder will be referenced by orchestrator technology <br>
1.1 `function/`
   1.1.1 Contains implementations of custom logic as 'function' that will run via orchestrator, such as `postgres_sql_function.py` and `snowflake_sql_function.py`
   1.1.2 All the functions will follow the API contract defined in `function_abstract.py`
   1.1.3 `requirement.txt` - Functions dependencies <br>
1.2 `job/` - Contains (technology agnostic) YAML job definitions, such as `derived_dataset_materialize_sink.yaml`
1.3 `sql/` - Contains SQL files (business logic) which will be referenced in job, such as `transform_source_to_sink.sql`
<br>

2. `airflow/` 
   Contains Airflow-specific code and configurations <br>
2.1.1 `dags/job_parser.py` - Script to generate Airflow DAGs from yamls under orchestration/job
2.1 `interface` - Contains all the logic to convert orchestration technology agnostic artifacts to orchestration technology specific artifacts
2.1.1 `function/function_to_operator_generator.py` - Script to convert functions under orchestration/function to airflow operators, this scipt is WIP
2.2 `operator/` - Contains custom Airflow Operators produced by script `interface/function/function_to_operator_generator.py`, such as `PostgresSQLFunction.py`
2.2 `secret/` - Folder to mock secret storage service
<br>

3. Root level files:
   - `README.md` - Documentation

