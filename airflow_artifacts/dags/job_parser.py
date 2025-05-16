"""
Job Parser for Airflow DAGs
This module is responsible for parsing YAML files and creating Airflow DAGs.
"""
import os
import importlib
import inspect
from typing import Dict, Any, List, Type
import yaml
from airflow import DAG
from airflow.models import BaseOperator
# from airflow_artifacts.operator.PostgresSQLFunction import PostgreSQLFunction

from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def days_ago(n: int) -> datetime:
    """Return a datetime object representing n days ago."""
    return datetime.now() - timedelta(days=n)

class JobParser:
    """Parser for job YAML configurations that creates Airflow DAGs."""
    
    def __init__(self, project_root_override: str = None):
        """
        Initialize the JobParser.
        
        Args:
            project_root_override: Override for the root directory of the project.
                                   If None, will try to detect automatically.
        """
        # Determine project root and key directories
        self.project_root = '/opt/airflow/data'
        self.job_dir = os.path.join(self.project_root, "orchestration", "jobs")
        self.operator_dir = os.path.join(self.project_root, "airflow_artifacts", "operator")
        
        print(f"JobParser Initialized:")
        print(f"  Project Root: {self.project_root}")
        print(f"  Job (YAML) Directory: {self.job_dir}")
        print(f"  Operator Directory: {self.operator_dir}")
        
        self.operator_map = self._load_operators()
        
        if self.operator_map:
            print(f"  Available operators: {list(self.operator_map.keys())}")
        else:
            print("  No custom operators loaded or operator directory not found.")
    
    def _load_operators(self) -> Dict[str, Type[BaseOperator]]:
        """
        Dynamically load all operator classes from the operator directory.
        """
        operator_map = {}
        if not os.path.exists(self.operator_dir): # Guard again if called separately
            return operator_map

        operator_files = [f for f in os.listdir(self.operator_dir) 
                         if f.endswith('.py') and not f.startswith('__')]
        
        for file_name in operator_files:
            module_name = file_name[:-3]
            try:
                # Module path for import should be relative to a PYTHONPATH entry.
                # In Docker, /opt/airflow is on PYTHONPATH, and operators are in /opt/airflow/airflow/operator
                # So, the import path is airflow.operator.module_name
                full_module_path = f'airflow_artifacts.operator.{module_name}'
                module = importlib.import_module(full_module_path)
                
                for name, obj in inspect.getmembers(module):
                    if (inspect.isclass(obj) and 
                        issubclass(obj, BaseOperator) and 
                        obj != BaseOperator):
                        operator_map[name] = obj
                        print(f"    Loaded operator: {name} from {full_module_path}")
            except ImportError as e:
                print(f"    WARNING: Failed to import operator module {full_module_path} from {file_name}: {str(e)}")
            except Exception as e:
                print(f"    WARNING: Failed to load operator from {file_name} (module: {full_module_path}): {str(e)}")
        return operator_map
    
    def load_yaml_config(self, yaml_file: str) -> Dict[str, Any]:
        """
        Load YAML configuration file.
        """
        if not os.path.exists(yaml_file):
            raise ValueError(f"YAML file not found: {yaml_file}")
        with open(yaml_file, 'r') as f:
            return yaml.safe_load(f)
    
    def get_operator_class(self, operator_name: str) -> Type[BaseOperator]:
        """
        Get the operator class by its name (as used in YAML).
        """
        if operator_name in self.operator_map:
            return self.operator_map[operator_name]
        raise ValueError(f"Operator '{operator_name}' not found in operator_map. Available: {list(self.operator_map.keys())}")
    
    def create_dag_from_config(self, config: Dict[str, Any], dag_file_path: str) -> DAG:
        """
        Create a DAG from configuration dictionary.
        The dag_file_path is important for Airflow to correctly associate the DAG object with a file.
        """
        dag = DAG(
            dag_id=config['job_id'],
            description=config.get('description', ''),
            schedule=config['schedule'],
            start_date=days_ago(100),
            catchup=config.get('catchup', False),
            tags=config.get('tags', []),
        )
        
        tasks = {}
        for task_id, task_config in config.get('tasks', {}).items():
            try:
                operator_class = self.get_operator_class(task_config['function'])

                kwargs = task_config.get('kwargs', {})
                sql_file_path = kwargs.pop('sql_file_path', None)
                if not sql_file_path:
                    raise ValueError(f"PostgreSQLFunction requires 'sql_file_path' in kwargs for task '{task_id}'")
                
                # Get secret_key from task_config (not from kwargs)
                secret_key = task_config.get('secret_key')
                if not secret_key:
                    raise ValueError(f"PostgreSQLFunction requires 'secret_key' for task '{task_id}'")
                
                # Create task with required parameters
                task = operator_class(
                    task_id=task_config['task_id'],
                    sql_file_path=sql_file_path,
                    use_dict_cursor=True,  # Default value from the operator
                    kwargs=kwargs,  # Pass remaining kwargs as a dictionary
                    secret_key=secret_key,  # Pass secret_key as a top-level parameter
                    dag=dag  # Pass DAG to BaseOperator
                )
                
                # # Handle PostgreSQLFunction specially
                # if operator_class == PostgreSQLFunction:
                #     # Extract required parameters
                #     kwargs = task_config.get('kwargs', {})
                #     sql_file_path = kwargs.pop('sql_file_path', None)
                #     if not sql_file_path:
                #         raise ValueError(f"PostgreSQLFunction requires 'sql_file_path' in kwargs for task '{task_id}'")
                    
                #     # Get secret_key from task_config (not from kwargs)
                #     secret_key = task_config.get('secret_key')
                #     if not secret_key:
                #         raise ValueError(f"PostgreSQLFunction requires 'secret_key' for task '{task_id}'")
                    
                #     # Create task with required parameters
                #     task = operator_class(
                #         task_id=task_config['task_id'],
                #         sql_file_path=sql_file_path,
                #         use_dict_cursor=True,  # Default value from the operator
                #         kwargs=kwargs,  # Pass remaining kwargs as a dictionary
                #         secret_key=secret_key,  # Pass secret_key as a top-level parameter
                #         dag=dag  # Pass DAG to BaseOperator
                #     )
                # else:
                #     # For other operators, pass kwargs directly
                #     task = operator_class(
                #         task_id=task_config['task_id'],
                #         dag=dag,  # Pass DAG directly
                #         **task_config.get('kwargs', {})
                #     )
                
                tasks[task_id] = task
                
            except Exception as e:
                print(f"ERROR creating task '{task_id}' for DAG '{config['job_id']}': {e}")
                # Optionally, re-raise or handle gracefully so other tasks/DAGs might load
                # For now, let it propagate if it's critical for this DAG
                raise
        
        for dep in config.get('dependencies', []):
            if dep['source'] in tasks and dep['target'] in tasks:
                tasks[dep['source']] >> tasks[dep['target']]
            else:
                print(f"WARNING: Invalid dependency for DAG '{config['job_id']}': {dep['source']} or {dep['target']} not found.")
        return dag
    
    def create_dag_from_yaml(self, yaml_file_path: str) -> DAG:
        """
        Create a DAG from YAML configuration file.
        """
        print(f"  Processing YAML: {yaml_file_path}")
        config = self.load_yaml_config(yaml_file_path)
        # Pass the yaml_file_path to be set as fileloc for the DAG
        return self.create_dag_from_config(config, dag_file_path=yaml_file_path)
    
    def load_all_dags(self) -> Dict[str, DAG]:
        """
        Load all DAGs from YAML files in the job directory.
        The job_dir is determined during __init__ based on environment.
        """
        dags = {}
        if not os.path.exists(self.job_dir):
            print(f"Skipping DAG loading: Job directory '{self.job_dir}' does not exist.")
            return dags
            
        print(f"Scanning for YAML DAGs in: {self.job_dir}")
        for filename in os.listdir(self.job_dir):
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                yaml_path = os.path.join(self.job_dir, filename)
                # Use the YAML filename (without extension) as part of the DAG ID or for logging
                # dag_id_from_filename = os.path.splitext(filename)[0]
                try:
                    # The DAG ID from within the YAML is the source of truth for dag_id
                    dag = self.create_dag_from_yaml(yaml_path)
                    if dag.dag_id in dags:
                        print(f"WARNING: Duplicate DAG ID '{dag.dag_id}' found. Overwriting DAG from {yaml_path}.")
                    dags[dag.dag_id] = dag # Store by actual dag_id from YAML
                    print(f"    Successfully loaded DAG: '{dag.dag_id}' from {filename}")
                except Exception as e:
                    print(f"    ERROR loading DAG from {filename}: {str(e)}")
        
        if not dags:
            print(f"No YAML DAGs found or loaded from {self.job_dir}")
        return dags

# --- Airflow DAG Discovery --- #
# This code runs when Airflow imports this file.
# It initializes the parser and injects the generated DAGs into the global scope.
print(f"Executing job_parser.py: Attempting to load and register DAGs...")
parser = JobParser() 
dags_to_register = parser.load_all_dags()

if dags_to_register:
    globals().update(dags_to_register)
    print(f"Registered {len(dags_to_register)} DAGs into globals: {list(dags_to_register.keys())}")
else:
    print("No DAGs were registered by job_parser.py.")
