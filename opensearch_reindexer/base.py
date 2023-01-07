from dataclasses import dataclass

import typer
from opensearchpy.helpers import bulk
try:
    from migrations.env import (
        source_client,
        destination_client,
    )
except ImportError:
    pass
import os
import re
import shutil
from rich import print


@dataclass
class Config:
    source_index: str
    destination_index: str
    destination_mappings: str = None


class BaseMigration:
    def __init__(
        self,
        config: Config = None
    ):
        self.config = config

    def get_revision_num_document(self):
        # Query the "reindexer_version" index and return the first document
        query = {
            "query": {
                "match_all": {}
            },
            "size": 1
        }
        search_response = source_client.search(index='reindexer_version', body=query)
        return search_response['hits']['hits'][0]

    def update_migration_version(self, new_version: int):
        """
        Update the migration version in the revision_num document.
        """

        remote_revision_num_document = self.get_revision_num_document()
        remote_revision_num_document_id = remote_revision_num_document["_id"]

        document = source_client.update(
            index="reindexer_version",
            id=remote_revision_num_document_id,
            body={
                "doc": {
                    "versionNum": new_version
                }
            },
            refresh="wait_for",
            _source=True,
        )["get"]
        print(f'"versionNum" updated from {remote_revision_num_document["_source"]["versionNum"]} to {document["_source"]["versionNum"]}')

    def get_remote_version_num(self) -> int:
        reindexer_num_source = self.get_revision_num_document()["_source"]
        # Get the "versionNum" field from the first document
        return int(reindexer_num_source['versionNum'])

    def get_revisions_to_execute(self, migration_files: list[str]) -> list[str]:
        remote_version_num = self.get_remote_version_num()

        # Get revisions that need to be executed
        revisions_to_execute = []
        for file in migration_files:
            version = int(re.search(r'\d+', file).group())
            if version > remote_version_num:
                revisions_to_execute.append(file)

        return sorted(revisions_to_execute)

    def transform_document(self, doc):
        # by default, don't transform'
        return doc

    def reindex(self):
        # Exit if source_index doesn't exist'
        if not source_client.indices.exists(index=self.config.source_index):
            print("Index " + self.config.source_index + " not exists")
            exit()

        # If the destination index does not exist, create it with the desired mappings
        if not source_client.indices.exists(index=self.config.destination_index):
            print("Destination Index " + self.config.destination_index + " doesn't exist. Creating it...")
            source_client.indices.create(
                index=self.config.destination_index,
                body=self.config.destination_mappings,
            )

        # Init scroll by search
        data = source_client.search(
            index=self.config.source_index,
            doc_type="_doc",
            scroll='2m',
            size=1000,
            body={}
        )

        # Get the scroll ID
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])

        while scroll_size > 0:
            print(f'Starting reindex from "{self.config.source_index}" to "{self.config.destination_index}"...')

            # Before scroll, process current batch of hits
            source_docs = data['hits']['hits']

            destination_docs = []
            for doc in source_docs:
                destination_doc = self.transform_document(doc["_source"])
                destination_docs.append(destination_doc)

            response = bulk(
                source_client,
                destination_docs,
                index=self.config.destination_index,
                refresh="wait_for",
            )
            print(response)

            data = source_client.scroll(scroll_id=sid, scroll='2m')

            # Update the scroll ID
            sid = data['_scroll_id']

            # Get the number of results that returned in the last scroll
            scroll_size = len(data['hits']['hits'])

        source_client.clear_scroll(scroll_id=sid)
        print(f'Reindex from "{self.config.source_index}" to "{self.config.destination_index}" complete')

    def handle_migration(self):
        revision_files = os.listdir('./migrations/versions')
        revisions_to_execute = self.get_revisions_to_execute(revision_files)

        # Check if a reindex is needed
        if len(revisions_to_execute) > 0:
            print(f"Revisions to be executed: {revisions_to_execute}")
            path = os.getcwd()
            for revision_file in revisions_to_execute:
                print(f'{path}/migrations/versions/{revision_file}')
                # Open the file as a TextIO object
                with open(f'{path}/migrations/versions/{revision_file}', 'r') as f:
                    # Read the contents of the file as a string
                    code = f.read()

                    # Reindex the data from the source index to the destination index
                    exec(code, globals())
                new_version = self.extract_version_from_file_name(revision_file)
                self.update_migration_version(new_version)

        else:
            print("Reindex not needed. Exiting.")
            exit()

    def extract_version_from_file_name(self, file_name):
        self.valid_file_name(file_name)
        version = int(re.search(r'\d+', file_name).group())
        return version

    def get_local_migration_version(self) -> int:
        # Check the "versions" directory for existing migration files
        revision_files = os.listdir('migrations/versions')

        # Find the highest version number among the existing migration files
        highest_version = 0
        for file in revision_files:
            self.valid_file_name(file)
            version = int(re.search(r'\d+', file).group())
            highest_version = max(highest_version, version)

        return highest_version

    @staticmethod
    def valid_file_name(file_name: str):
        if file_name.count('.') > 2:
            print(f'[bold red]found dots or numbers in file "{file_name}"[/bold red]')
            raise typer.Exit()

    def create_revision(self, m: str):
        highest_version = self.get_local_migration_version()

        # Create a new migration file with the next highest version number
        new_version = highest_version + 1

        # create revision file
        revision_file_name = f'migrations/versions/{new_version}_{m}.py'
        shutil.copy(f'migrations/migration_template.py', revision_file_name)
        return revision_file_name
