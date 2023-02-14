import importlib.util
import os
from typing import Union

from opensearchpy import OpenSearch


def dynamically_import_migrations() -> Union[
    tuple[OpenSearch, OpenSearch, str], tuple[None, None]
]:
    """
    Dynamically imports the necessary migration files and returns the 'source_client' from the 'env.py' file.
    """
    try:
        # Obtain the file's path
        current_working_dir = os.getcwd()
        migrations_dir = os.path.join(current_working_dir, "migrations")
        init_file_path = os.path.join(migrations_dir, "__init__.py")
        file_path = os.path.join(migrations_dir, "env.py")

        # Create a ModuleSpec object
        init_spec = importlib.util.spec_from_file_location("init", init_file_path)
        spec = importlib.util.spec_from_file_location("env", file_path)

        # Load the module
        init = importlib.util.module_from_spec(init_spec)
        env = importlib.util.module_from_spec(spec)

        init_spec.loader.exec_module(init)
        spec.loader.exec_module(env)

        return env.source_client, env.destination_client, env.VERSION_CONTROL_INDEX
    except FileNotFoundError:
        pass
    return None, None
