import os
import re
import shutil
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

import opensearchpy.exceptions
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
from rich import print


class Language(Enum):
    python = "python"
    painless = "painless"


@dataclass
class Config:
    source_index: str = None
    destination_index: str = None
    batch_size: int = 1000
    destination_index_body: Optional[dict] = None
    language: Language = Language.painless
    reindex_body: dict = None


class BaseMigration:
    def __init__(
        self,
        config: Config = None,
    ):
        self.config = config

        from opensearch_reindexer.db import dynamically_import_migrations

        (
            source_client,
            destination_client,
            version_control_index,
        ) = dynamically_import_migrations()

        self.source_client: OpenSearch = source_client
        self.destination_client: OpenSearch = destination_client
        self.version_control_index: str = version_control_index

        if config and config.language == Language.painless:
            config.source_index = config.reindex_body["source"]["index"]
            config.destination_index = config.reindex_body["dest"]["index"]

    def on_setup(self):
        pass

    def before_revision(self):
        pass

    def after_revision(self):
        pass

    def on_complete(self):
        pass

    def get_revisions(self):
        """Returns an ordered list of revisions."""
        return sorted(
            [f for f in os.listdir("migrations/versions") if f.endswith(".py")]
        )

    def get_revision_num_document(self):
        # Query the "reindexer_version" index and return the first document
        query = {"query": {"match_all": {}}, "size": 1}
        search_response = self.source_client.search(
            index=self.version_control_index, body=query
        )
        return search_response["hits"]["hits"][0]

    def update_migration_version(self, new_version: int):
        """
        Update the migration version in the revision_num document.
        """
        remote_revision_num_document = self.get_revision_num_document()
        remote_revision_num_document_id = remote_revision_num_document["_id"]

        self.source_client.index(
            index=self.version_control_index,
            id=remote_revision_num_document_id,
            body={"versionNum": new_version},
            refresh="wait_for",
        )

        source_host = self.source_client.transport.hosts[0]["host"]
        destination_host = self.destination_client.transport.hosts[0]["host"]

        # Check if we are reindexing from one cluster to another
        if source_host != destination_host:
            print(
                f"You have reindexed from one cluster to another. We will update '{self.version_control_index}' in both clusters."
            )
            self.destination_client.indices.create(
                index=self.version_control_index, ignore=400
            )

            self.destination_client.index(
                index=self.version_control_index,
                id=remote_revision_num_document_id,
                body={"versionNum": new_version},
                refresh="wait_for",
            )

        print(
            f'"versionNum" was updated from {remote_revision_num_document["_source"]["versionNum"]} to {new_version}'
        )

    def get_remote_version_num(self) -> int:
        reindexer_num_source = self.get_revision_num_document()["_source"]
        # Get the "versionNum" field from the first document
        return int(reindexer_num_source["versionNum"])

    def get_revisions_to_execute(self) -> List[str]:
        try:
            revision_files = self.get_revisions()
            if not revision_files:
                print(
                    'No revision files found in "./migrations/versions".\nPlease create one by running: "reindexer revision"'
                )
                exit(1)

            remote_version_num = self.get_remote_version_num()
            revisions_to_execute = [
                file
                for file in revision_files
                if int(re.search(r"\d+", file).group()) > remote_version_num
            ]

            if not revisions_to_execute:
                print("No new revisions found.")
                exit(0)

            return revisions_to_execute
        except FileNotFoundError:
            print(
                'Please perform the following steps before running "reindexer list":\n'
                '1. Run "reindexer init"\n'
                '2. Run "reindexer init-index"\n'
                '3. Configure source_client in "./migrations/env.py"'
            )
            exit(1)

    def transform_document(self, doc):
        # by default, don't transform'
        return doc

    def reindex(self):
        # Exit if source_index doesn't exist'
        if (
            self.config.source_index is not None
            and not self.source_client.indices.exists(
                index=self.config.source_index,
            )
        ):
            print("Source index " + self.config.source_index + " not exist.")
            exit(1)

        # If the destination index does not exist, create it with the desired mappings
        if not self.source_client.indices.exists(index=self.config.destination_index):
            print(
                "Destination index "
                + self.config.destination_index
                + " doesn't exist. Creating it..."
            )
            self.source_client.indices.create(
                index=self.config.destination_index,
                body=self.config.destination_index_body,
            )

        if self.config.source_index is None:
            print("Source index was None, skipping reindexing")
            return

        if self.config.language == Language.painless:
            self.reindex_painless()
        else:
            self.reindex_python()
        print(
            f'Reindex from "{self.config.source_index}" to "{self.config.destination_index}" complete'
        )

    def reindex_painless(self):
        try:
            response = self.source_client.reindex(
                body=self.config.reindex_body,
                refresh=True,
            )
            print(response)
        except opensearchpy.exceptions.RequestError as e:
            print(e)
            raise e

    def reindex_python(self):
        # Init scroll by search
        data = self.source_client.search(
            index=self.config.source_index,
            scroll="2m",
            size=self.config.batch_size,
            body={},
        )

        # Get the scroll ID
        sid = data["_scroll_id"]
        scroll_size = len(data["hits"]["hits"])

        while scroll_size > 0:
            print(
                f'Starting reindex from "{self.config.source_index}" to "{self.config.destination_index}"...'
            )

            # Before scroll, process current batch of hits
            source_docs = data["hits"]["hits"]

            destination_docs = []
            for doc in source_docs:
                destination_doc = self.transform_document(doc["_source"])
                destination_docs.append(destination_doc)

            response = bulk(
                self.destination_client,
                destination_docs,
                index=self.config.destination_index,
                refresh="wait_for",
            )
            print(response)

            data = self.source_client.scroll(scroll_id=sid, scroll="2m")

            # Update the scroll ID
            sid = data["_scroll_id"]

            # Get the number of results that returned in the last scroll
            scroll_size = len(data["hits"]["hits"])

        self.source_client.clear_scroll(scroll_id=sid)

    def read_and_exec_file(self, file_path):
        with open(file_path, "r") as file:
            code = file.read()
            exec(code, globals())

    def handle_migration(self):
        if not self.source_client.indices.exists(index=self.version_control_index):
            print(
                f'Version control index "{self.version_control_index}" does not exist.\nCreate it by running "reindexer init-index"'
            )
            exit(1)

        revisions_to_execute = self.get_revisions_to_execute()

        if len(revisions_to_execute) > 0:
            self.on_setup()
            print(f"Revisions to be executed: {revisions_to_execute}")
            path = os.getcwd()
            for revision_file in revisions_to_execute:
                file_path = os.path.join(path, "migrations/versions", revision_file)
                print(file_path)

                # Dynamically import the revision file
                self.read_and_exec_file(file_path)

                migration = Migration(config)
                migration.before_revision()
                # Execute migration
                migration.reindex()
                migration.after_revision()

                new_version = self.extract_version_from_file_name(revision_file)
                self.update_migration_version(new_version)
        else:
            print("All revisions are up to date.")
        self.on_complete()

    def extract_version_from_file_name(self, file_name):
        self.valid_file_name(file_name)
        version = int(re.search(r"\d+", file_name).group())
        return version

    def get_local_migration_version(self) -> int:
        # Check the "versions" directory for existing migration files
        revision_files = self.get_revisions()

        # Find the highest version number among the existing migration files
        highest_version = 0
        for file in revision_files:
            self.valid_file_name(file)
            version = int(re.search(r"\d+", file).group())
            highest_version = max(highest_version, version)

        return highest_version

    @staticmethod
    def valid_file_name(file_name: str):
        if file_name.count(".") > 1:
            print(f'[bold red]found dots or numbers in file "{file_name}"[/bold red]')
            exit(1)

    def create_revision(self, m: str, l: Language = Language.painless):
        highest_version = self.get_local_migration_version()

        # Create a new migration file with the next highest version number
        new_version = highest_version + 1

        # create revision file
        revision_file_name = f"migrations/versions/{new_version}_{m}.py"

        shutil.copy(f"migrations/migration_template_{l.value}.py", revision_file_name)
        return revision_file_name
