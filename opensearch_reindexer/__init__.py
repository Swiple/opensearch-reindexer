import typer
from pathlib import Path
from opensearch_reindexer.base import BaseMigration
from opensearchpy import OpenSearch
from rich import print
import importlib.util
import os

app = typer.Typer()


@app.command()
def init():
    """
    Creates the folders and files needed by "opensearch-reindexer"

    Creates the following
    migrations/
        env.py
        migration_template.py
        versions/
            add_account_1.py
            add_order_id_2.py
            rename_username_field_3.py
    """
    # Create the directory if it doesn't exist
    Path('./migrations').mkdir(parents=True, exist_ok=True)
    Path('./migrations/__init__.py').write_text("")
    Path('./migrations/versions').mkdir(parents=True, exist_ok=True)
    Path('./migrations/migration_template.py').write_text("""from opensearch_reindexer.base import BaseMigration, Config

SOURCE_INDEX = ""
DESTINATION_INDEX = ""
DESTINATION_MAPPINGS = None


class Migration(BaseMigration):
    def transform_document(self, doc: dict) -> dict:
        # Modify this method to transform each document before being inserted into destination index.
        return doc


config = Config(
    source_index=SOURCE_INDEX,
    destination_index=DESTINATION_INDEX,
)
Migration(config).reindex()
    """)
    Path('./migrations/env.py').write_text("""from opensearchpy import OpenSearch

OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = "9200"
OPENSEARCH_USERNAME = "admin"
OPENSEARCH_PASSWORD = "admin"
OPENSEARCH_USE_SSL = True
OPENSEARCH_VERIFY_CERTS = False


# Create the client with SSL/TLS enabled, but hostname verification disabled.
source_client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
    use_ssl=OPENSEARCH_USE_SSL,
    verify_certs=OPENSEARCH_VERIFY_CERTS,
    ssl_show_warn=False,
)

destination_client = source_client
    """)


@app.command()
def init_index():
    source_client = dynamically_import_migrations()

    version_index = 'reindexer_version'

    if source_client.indices.exists(index=version_index):
        source_client.search(index=version_index)
        print(f'[bold red]Index "{version_index}" already exists[/bold red]')
        raise typer.Exit()

    source_client.indices.create(
        index=version_index,
    )
    highest_version = BaseMigration().get_local_migration_version()

    if highest_version != 0:
        print("Local revisions were detected. We assume you have migrated from one OpenSearch Cluster to another.")

    print(f'"versionNum" was initialized to {highest_version}')
    source_client.index(index=version_index, body={"versionNum": highest_version})


@app.command()
def revision(m: str):
    """
    Creates a new revision/migration file.

    :param m: string message to apply to the revision; this is the
     ``-m`` option to ``reindexer revision``.
    """
    message = m.replace(' ', '_')
    BaseMigration.valid_file_name(message)
    BaseMigration().create_revision(message)


@app.command()
def run():
    """
    Runs 0 or many migrations
    """
    BaseMigration().handle_migration()


def dynamically_import_migrations() -> OpenSearch:
    # Obtain the file's path
    current_working_dir = os.getcwd()
    file_path = os.path.join(current_working_dir, 'migrations', 'env.py')

    # Create a ModuleSpec object
    spec = importlib.util.spec_from_file_location('env', file_path)

    # Load the module
    env = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(env)

    return env.source_client