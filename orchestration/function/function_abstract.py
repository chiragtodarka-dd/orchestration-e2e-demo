from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, Union
from dataclasses import dataclass
from datetime import datetime
import json
import os

class ExecutionContext:
    """Context object that contains execution-related information."""
    execution_date: datetime
    task_id: str
    dag_id: str
    run_id: str
    params: Dict[str, Any]
    secret_key: Optional[str] # Can be the name of the default secret file, e.g., "postgres_credentials.json"

    def __init__(
        self,
        execution_date: datetime,
        task_id: str,
        dag_id: str,
        run_id: str,
        params: Dict[str, Any] = None,
        secret_key: Optional[str] = None
    ):
        self.execution_date = execution_date
        self.task_id = task_id
        self.dag_id = dag_id
        self.run_id = run_id
        self.params = params or {}
        self.secret_key = secret_key

    def get_secret(self, secret_name_override: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch a secret value from the secrets directory.
        Uses secret_name_override if provided, otherwise defaults to self.secret_key.
        
        Args:
            secret_name_override: Optional name of the secret file (e.g., "my_specific_secret" or "my_specific_secret.json")
            
        Returns:
            Dict[str, Any]: The secret values as a dictionary
            
        Raises:
            ValueError: If no secret name can be determined.
            FileNotFoundError: If the secret file doesn't exist.
            json.JSONDecodeError: If the secret file is not valid JSON.
        """
        name_to_fetch = secret_name_override or self.secret_key
        
        if not name_to_fetch:
            raise ValueError("No secret name provided and no default secret_key set in ExecutionContext.")

        # Ensure we use the base name without .json for constructing the path
        if name_to_fetch.endswith(".json"):
            base_secret_name = os.path.splitext(name_to_fetch)[0]
        else:
            base_secret_name = name_to_fetch
            
        # Path assumes function_abstract.py is in airflow/interface/function/
        # and secrets are in airflow/secret/
        secret_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'secret',
            f"{base_secret_name}.json"
        )
        
        if not os.path.exists(secret_path):
            raise FileNotFoundError(f"Secret file not found: {secret_path} (derived from key: '{name_to_fetch}')")
            
        with open(secret_path, 'r') as f:
            return json.load(f)

class Function(ABC):
    """
    Abstract base class for implementing pure Python business logic.
    This class is designed to be independent of Airflow primitives,
    allowing for better separation of concerns and testability.
    """
    
    def __init__(self, context: ExecutionContext, **kwargs):
        """
        Initialize the function with configuration parameters.
        
        Args:
            context: ExecutionContext object containing execution information
            **kwargs: Configuration parameters specific to the function implementation
                      This may include a specific 'secret_file' or similar key 
                      if the function needs a secret different from context.secret_key.
        """
        if context is None:
            raise ValueError("ExecutionContext cannot be None")
        self.context = context
        self.config = kwargs # kwargs passed from Operator, potentially containing specific secret file name
    
    def _validate_params(self) -> bool:
        """
        Internal method to validate that all required parameters are present and have correct types.
        Subclasses should override this and _get_required_params if they have specific params.
        
        Returns:
            bool: True if all parameters are valid, False otherwise
        """
        required_params = self._get_required_params()
        for param in required_params:
            if param not in self.config:
                raise ValueError(f"Required parameter '{param}' for function '{self.__class__.__name__}' is missing in config: {self.config}")
        return True
    
    def _get_required_params(self) -> List[str]:
        """
        Internal method to get the list of required configuration parameters for the function.
        Subclasses should override this to specify their required params.
        
        Returns:
            List[str]: List of required parameter names
        """
        return [] # Default to no required params
    
    @abstractmethod
    def pre_execute(self) -> None:
        """
        Hook method called before execution.
        Override this method to perform any pre-execution tasks (e.g., self._validate_params()).
        """
        pass
    
    @abstractmethod
    def execute(self) -> Any:
        """
        Execute the main business logic.
        Function implementations will use self.context.get_secret() or 
        self.context.get_secret(self.config.get('specific_secret_key_name')) to get credentials.
        """
        pass
    
    @abstractmethod
    def post_execute(self) -> None:
        """
        Hook method called after successful execution.
        Override this method to perform any post-execution tasks.
        """
        pass
    
    @abstractmethod
    def on_success(self) -> None:
        """
        Hook method called when execution is successful.
        Override this method to perform any success-related tasks.
        """
        pass
    
    @abstractmethod
    def on_failure(self) -> None:
        """
        Hook method called when execution fails.
        Override this method to perform any failure-related tasks.
        """
        pass
    
    @abstractmethod
    def on_retry(self) -> None:
        """
        Hook method called when execution is being retried.
        Override this method to perform any retry-related tasks.
        """
        pass 