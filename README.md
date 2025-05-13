#### Philosophy
   - Clear separation between business logic and technical implementation
   - A workflow is a collection of inter-dependent tasks, a workflow is generically referred as job in technology agnostic terminology
   - Client/end user 
      - Provide implementation of custom logic as 'functions' that can be run via orchestrator. Function are the building block on which task are build
      - Define the workflow as job definition and provide required business logic (sql)
   - Orchestration team 
      - Responsible to own and maintain the orchestration infrastructure
      - Will select appropriate technology (such as from Airflow/Dagster/other) as per the business needs
      - Will provide and own the logic to translate technology agnostic artifacts to technology specific artifacts
   - If/when orchestration team see the need of changing underlying technology such as from Airflow to Dagster or any other combination, it will provide and own the logic to convert technology agnostic artifacts to new technology specific artifacts
   - Based on this philosophy and design changing the underlying orchestration technology will not be exposed to users and there will be no effort at end users side

#### Note
- For this demo using Airflow as choice for orchestration but in production it can be any other orchestration too

#### Key Components and Project Structure:
```
orchestration-e2e-demo/
├── orchestration/	
│   │ # Contains orchestration technology agnostic artifacts
│   │ # Clients/end users will be primary contributors in this folder
│   │ # This folder will be referenced by orchestrator technology
│   │
│   ├── function/ 					# Contains implementations of custom logic as 'function'
│   │   ├── function_abstract.py 			# All the functions will follow the API contract defined in function_abstract.py
│   │   ├── postgres_sql_function.py 			# Concrete implementation of function_abstract.py to execute SQL in Postgres
│   │   └── requirements.txt 				# Functions dependencies
│   ├── job/ 						# Contains (technology agnostic) YAML job definition
│   │   └── derived_dataset_materialize_sink.yaml 	# Sample job to materialize 'sink' table based of a transformation on 'source' table
│   └── sql/ 						# Contains SQL files (business logic) which will be referenced in job
│       └── transform_source_to_sink.sql 		# Transformation logic to be used in yaml job
│
└── airflow/					        # Contains Airflow-specific code and configurations
    ├── dags/
    │   └── job_parser.py 				# Script to generate Airflow DAGs from yamls under orchestration/job
    ├── operator/ 					# Contains custom Airflow Operators
    │   └── postgresqlfunction.py 			# Airflow operator build from orchestration/function/postgres_sql_function.py
    └── secret/ 					# Folder to mock secret storage service
        └── postgres_credentials.json
```

