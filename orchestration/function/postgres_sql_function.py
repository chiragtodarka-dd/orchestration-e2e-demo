from typing import Any, Dict, List, Optional
import psycopg2
import os
from psycopg2.extras import RealDictCursor

from orchestration.function.function_abstract import Function, ExecutionContext # Adjusted import path

class PostgreSQLFunction(Function):
    """
    Implementation of the Function abstract class for PostgreSQL operations.
    Executes SQL from a specified file using credentials from a secret file.
    """
    
    def __init__(self, context: ExecutionContext, sql_file_path: str, use_dict_cursor: bool = True, **kwargs):
        """
        Initialize the PostgreSQL function.
        
        Args:
            context: ExecutionContext object containing execution information (including secret_key).
            sql_file_path: Path to the .sql file to be executed.
            use_dict_cursor: Whether to use RealDictCursor for query results.
            **kwargs: Additional configuration parameters.
        """
        super().__init__(context, **kwargs) # Pass all other kwargs to base
        self.config['sql_file_path'] = sql_file_path # Store sql_file_path in config for access
        self.config['use_dict_cursor'] = use_dict_cursor
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None
        self.sql_to_execute: Optional[str] = None
        self.params = kwargs.get('kwargs', {})  # Get params from kwargs

    def _get_required_params(self) -> List[str]:
        return ['sql_file_path'] # sql_file_path is now explicitly managed via __init__ and self.config

    def _get_connection_params_from_secret(self) -> Dict[str, Any]:
        """Fetches connection parameters from the secret file specified in context.secret_key."""
        if not self.context.secret_key:
            raise ValueError("secret_key not provided in ExecutionContext, cannot fetch credentials.")
        creds = self.context.get_secret() # Uses self.context.secret_key by default
        # Expected keys: host, port, database, user, password
        required_keys = ['host', 'database', 'user', 'password']
        for key in required_keys:
            if key not in creds:
                raise ValueError(f"Missing required key '{key}' in secret file '{self.context.secret_key}'")
        return creds

    def _read_sql_file(self) -> str:
        """Reads SQL content from the file specified by sql_file_path."""
        # Construct absolute path if sql_file_path is relative to project root or a specific dir
        # Assuming sql_file_path might be relative to project root for now.
        # If job definitions are in orchestration/job and SQL in orchestration/sql,
        # and dag_factory/job_parser is running from airflow/dags or airflow/interface
        # we need a robust way to resolve this path. For now, let's assume it can be resolved
        # or it's an absolute path.
        # For simplicity, let's assume it's relative to where Airflow DAGs are processed from (project root usually)
        # A more robust solution would involve passing project_root or making paths absolute in YAML.
        sql_file = self.config['sql_file_path']
        sql_file = os.path.join('/opt/airflow/data', sql_file)
        
        if not os.path.isabs(sql_file):
             # This assumes the script/DAG is run from project root or PYTHONPATH includes it.
             # A common pattern is to define a base path in Airflow variables or make paths in YAML absolute.
             # For this exercise, we will assume the path is resolvable from the current working directory
             # or is made absolute before being passed to the operator.
             # For the generator, this path is just a string value.
             # When the DAG runs, this path needs to be accessible by the Airflow worker.
            pass # Keep it as is, expect it to be resolvable by the worker
        
        if not os.path.exists(sql_file):
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        with open(sql_file, 'r') as f:
            return f.read()

    def pre_execute(self) -> None:
        """
        Prepare for execution: validate params, read SQL, establish connection.
        """
        self._validate_params() # Validates sql_file_path from config
        self.sql_to_execute = self._read_sql_file()

        conn_params = self._get_connection_params_from_secret()
        self.conn = psycopg2.connect(
            host=conn_params['host'],
            port=conn_params.get('port', 5432),
            database=conn_params['database'],
            user=conn_params['user'],
            password=conn_params['password']
        )
        cursor_factory = RealDictCursor if self.config.get('use_dict_cursor', True) else None
        self.cursor = self.conn.cursor(cursor_factory=cursor_factory)
        print(f"PostgreSQL connection established for function: {self.__class__.__name__}")

    def execute(self) -> Any:
        """
        Execute the SQL query read from the file.
        """
        if not self.conn or not self.cursor or not self.sql_to_execute:
            raise RuntimeError("Connection or SQL not established/loaded. Call pre_execute first.")
        
        print(f"Executing SQL for function {self.__class__.__name__}:\n{self.sql_to_execute}...")
        print(f"With parameters: {self.params}")
        try:
            self.cursor.execute(self.sql_to_execute, self.params)
            if self.cursor.description:  # If the query returns results (SELECT)
                return self.cursor.fetchall()
            self.conn.commit() # For DML/DDL statements
            print("SQL executed and committed successfully.")
            return None
        except Exception as e:
            if self.conn: # Check if conn is not None before rollback
                self.conn.rollback()
            print(f"Error executing SQL query: {str(e)}. Transaction rolled back.")
            raise RuntimeError(f"Error executing SQL query: {str(e)}")

    def post_execute(self) -> None:
        """
        Clean up resources after execution.
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.cursor = None
        self.conn = None
        print("PostgreSQL connection closed.")

    def on_success(self) -> None:
        print(f"Function {self.__class__.__name__} executed successfully.")
        # self.post_execute() is usually called by the operator after execute completes without error

    def on_failure(self) -> None:
        print(f"Function {self.__class__.__name__} failed.")
        # Ensure resources are cleaned up even on failure, post_execute handles this.
        # The operator's general try/except should call this, then ensure post_execute (or equivalent cleanup)
        if self.conn and not self.conn.closed: # Check if conn exists and is not already closed
            try:
                self.conn.rollback() # Attempt rollback if connection still open
                print("Transaction rolled back due to failure.")
            except Exception as rb_exc:
                print(f"Error during rollback on failure: {rb_exc}")
        self.post_execute() # Ensure cleanup

    def on_retry(self) -> None:
        print(f"Retrying function {self.__class__.__name__}.")
        self.post_execute() # Clean up before retry
        # pre_execute will be called again by the operator or control logic before the next attempt 