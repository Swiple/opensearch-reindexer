import os
from pathlib import Path
from typing import Literal, Union

import typer
from rich import print

from opensearch_reindexer.base import BaseMigration, Language

app = typer.Typer()


@app.command()
def init():
    """
    Initializes the necessary folders and files for "opensearch-reindexer".

    This creates the following directories and files:
    migrations/
        env.py
        migration_template_python.py
        migration_template_painless.py
        versions/
            add_account_1.py
            add_order_id_2.py
            rename_username_field_3.py
    """
    # Create the directory if it doesn't exist
    Path("./migrations").mkdir(parents=True, exist_ok=True)
    Path("./migrations/__init__.py").write_text("")
    Path("./migrations/versions").mkdir(parents=True, exist_ok=True)
    Path("./migrations/migration_template_python.py").write_text(
        """from opensearch_reindexer.base import BaseMigration, Config, Language

# number of documents to index at a time
BATCH_SIZE = 1000
SOURCE_INDEX = ""
DESTINATION_INDEX = ""
DESTINATION_INDEX_BODY = None


class Migration(BaseMigration):
    def transform_document(self, doc: dict) -> dict:
        # Modify this method to transform each document before being inserted into destination index.
        return doc


config = Config(
    source_index=SOURCE_INDEX,
    destination_index=DESTINATION_INDEX,
    batch_size=BATCH_SIZE,
    destination_index_body=DESTINATION_INDEX_BODY,
    language=Language.python,
)
Migration(config).reindex()
    """
    )
    Path("./migrations/migration_template_painless.py").write_text(
        """from opensearch_reindexer.base import BaseMigration, Config, Language

REINDEX_BODY = {
    "source": {"index": "source"},
    "dest": {"index": "destination"},
}
DESTINATION_INDEX_BODY = None


config = Config(
    reindex_body=REINDEX_BODY,
    destination_index_body=DESTINATION_INDEX_BODY,
    language=Language.painless,
)
BaseMigration(config).reindex()
        """
    )
    Path("./migrations/env.py").write_text(
        """from opensearchpy import OpenSearch


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
    """
    )


@app.command()
def init_index():
    """
    Initializes the 'reindexer_version' index in the 'source_client' with the highest local revision version number.
    """
    # First check if `reindexer init` has been run.
    if not os.path.exists("migrations/versions"):
        print(
            'No migrations/versions directory found.\n\nInitialize your project first:\n1. "reindexer init"'
        )
        raise typer.Exit(1)

    from opensearch_reindexer.db import dynamically_import_migrations

    source_client, _ = dynamically_import_migrations()
    version_index = "reindexer_version"

    if source_client.indices.exists(index=version_index):
        source_client.search(index=version_index)
        print(f'[bold red]Index "{version_index}" already exists[/bold red]')
        raise typer.Exit(1)

    source_client.indices.create(
        index=version_index,
    )
    highest_version = BaseMigration().get_local_migration_version()

    if highest_version != 0:
        print(
            "Local revisions were detected. We assume you have migrated from one OpenSearch Cluster to another."
        )

    print(f'"versionNum" was initialized to {highest_version}')
    source_client.index(
        index=version_index,
        body={"versionNum": highest_version},
        refresh="wait_for",
    )


@app.command()
def revision(
    message: str,
    language: str = Language.painless.value,
):
    """
    Creates a new revision/migration file.

    :param message: string message to apply to the revision; this is the
     ``message`` option to ``reindexer revision``.
     :param language: Language - An enum value that controls whether reindexing
        should be performed natively by OpenSearch or by Python locally
    """
    try:
        lang = Language[language]
    except KeyError:
        print(f'Expected a language of "painless" or "python" but got {language}')
        raise typer.Exit(1)
    message = message.replace(" ", "_")
    BaseMigration.valid_file_name(message)
    BaseMigration().create_revision(message, lang)


@app.command()
def list():
    """
    Returns ordered list of revisions that have not been executed.
    """
    from opensearch_reindexer.db import dynamically_import_migrations

    source_client, _ = dynamically_import_migrations()
    revisions = BaseMigration().get_revisions_to_execute()
    for rev in revisions:
        print(rev)


@app.command()
def run():
    """
    Runs 0 or many migrations returned by `BaseMigration().get_revisions_to_execute()
    """
    BaseMigration().handle_migration()
