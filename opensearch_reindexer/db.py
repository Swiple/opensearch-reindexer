import importlib.util
import os
from typing import Union

from opensearchpy import OpenSearch


def dynamically_import_migrations() -> Union[
    tuple[OpenSearch, OpenSearch], tuple[None, None]
]:
    """
    Dynamically imports the necessary migration files and returns the 'source_client' from the 'env.py' file.
    """
    try:
        # Obtain the file's path
        current_working_dir = os.getcwd()
        file_path = os.path.join(current_working_dir, "migrations", "env.py")

        # Create a ModuleSpec object
        spec = importlib.util.spec_from_file_location("env", file_path)

        # Load the module
        env = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(env)
        return env.source_client, env.destination_client
    except FileNotFoundError:
        pass
    return None, None
