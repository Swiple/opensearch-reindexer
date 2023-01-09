import json
import os
import shutil
import unittest

from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError
from opensearchpy.helpers import bulk

REINDEXER_VERSION = "reindexer_version"
REINDEXER_SOURCE_INDEX = "reindexer_source_index"
REINDEXER_REVISION_1 = "reindexer_revision_1"
REINDEXER_REVISION_2 = "reindexer_revision_2"
REINDEXER_REVISION_3 = "reindexer_revision_3"


class TestOpensearchReindexer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source_client = cls.get_os_client()
        cls.delete_index(source_client, REINDEXER_VERSION)
        cls.delete_index(source_client, REINDEXER_SOURCE_INDEX)
        cls.delete_index(source_client, REINDEXER_REVISION_1)
        cls.delete_index(source_client, REINDEXER_REVISION_2)
        cls.delete_index(source_client, REINDEXER_REVISION_3)

        cls.load_reindexer_source_index(source_client, REINDEXER_SOURCE_INDEX)

        if os.path.exists("./migrations"):
            shutil.rmtree("./migrations")

    def test_setup_and_run_revisions(self):
        import opensearch_reindexer as osr

        source_client = self.get_os_client()

        osr.init()

        # verify that "reindexer_version" index is initialized to version 0 when no revisions exist
        osr.init_index()
        assert self.search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 0}

        # create revisions
        osr.revision("revision_1")
        osr.revision("revision_2")
        osr.revision("revision_3")

        # modify revision files source/destination indices, mappings and transform_document
        self.modify_revision_files()

        osr.run()

        # The # docs in revision indices should match "reindexer_source_index"
        expected_count = source_client.count(index=REINDEXER_SOURCE_INDEX)["count"]
        assert (
            source_client.count(index=REINDEXER_REVISION_1)["count"] == expected_count
        )
        assert (
            source_client.count(index=REINDEXER_REVISION_2)["count"] == expected_count
        )
        assert (
            source_client.count(index=REINDEXER_REVISION_3)["count"] == expected_count
        )

        # verify that documents were transformed as expected for each revision
        assert self.search(
            client=source_client,
            index=REINDEXER_REVISION_1,
        ) == {"a": 1, "b": 1, "c": {"a": "a", "b": "b", "c": 2}}

        assert self.search(
            client=source_client,
            index=REINDEXER_REVISION_2,
        ) == {"a": 1, "b": 1, "c": json.dumps({"a": "a", "b": "b", "c": 2})}

        assert self.search(
            client=source_client,
            index=REINDEXER_REVISION_3,
        ) == {"a": 1, "c": json.dumps({"a": "a", "b": "b", "c": 2})}

        # should find latest revision and initialize "reindexer_version to it if cluster has not been initialized.
        # This occurs when someone has re-indexed from one cluster to another
        source_client.indices.delete(index=REINDEXER_VERSION)
        osr.init_index()
        assert self.search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 3}

    def modify_revision_files(self):
        revision_one_mappings = {
            "properties": {
                "a": {"type": "long"},
                "b": {"type": "long"},
                "c": {
                    "properties": {
                        "a": {
                            "type": "text",
                        },
                        "b": {
                            "type": "text",
                        },
                        "c": {"type": "long"},
                    }
                },
            }
        }

        self.modify_revision_file(
            file_name="1_revision_1",
            modifications=[
                ['SOURCE_INDEX = ""', f"SOURCE_INDEX = '{REINDEXER_SOURCE_INDEX}'"],
                [
                    'DESTINATION_INDEX = ""',
                    f"DESTINATION_INDEX = '{REINDEXER_REVISION_1}'",
                ],
                [
                    "DESTINATION_MAPPINGS = None",
                    f"DESTINATION_MAPPINGS = {revision_one_mappings}",
                ],
            ],
        )

        revision_two_code = """
                import json
                doc['c'] = json.dumps(doc['c'])
                return doc
                """
        self.modify_revision_file(
            file_name="2_revision_2",
            modifications=[
                ['SOURCE_INDEX = ""', f"SOURCE_INDEX = '{REINDEXER_REVISION_1}'"],
                [
                    'DESTINATION_INDEX = ""',
                    f"DESTINATION_INDEX = '{REINDEXER_REVISION_2}'",
                ],
                ["return doc", revision_two_code],
            ],
        )
        revision_three_code = """
                del doc['b']
                return doc
                """
        self.modify_revision_file(
            file_name="3_revision_3",
            modifications=[
                ['SOURCE_INDEX = ""', f"SOURCE_INDEX = '{REINDEXER_REVISION_2}'"],
                [
                    'DESTINATION_INDEX = ""',
                    f"DESTINATION_INDEX = '{REINDEXER_REVISION_3}'",
                ],
                ["return doc", revision_three_code],
            ],
        )

    @staticmethod
    def get_os_client():
        return OpenSearch(
            hosts=[{"host": "localhost", "port": 9200}],
            http_compress=True,
            http_auth=("admin", "admin"),
            use_ssl=True,
            verify_certs=False,
            ssl_show_warn=False,
        )

    @staticmethod
    def load_reindexer_source_index(client, index):
        identifier = 0
        for i in range(2):
            docs = []
            for j in range(1000):
                identifier += 1
                doc = {"a": identifier, "b": 1, "c": {"a": "a", "b": "b", "c": 2}}
                docs.append(doc)

            response = bulk(
                client,
                docs,
                index=index,
                refresh="wait_for",
            )
            print(response)

    @staticmethod
    def delete_index(client, index):
        try:
            client.indices.delete(index)
        except NotFoundError:
            pass

    @staticmethod
    def modify_revision_file(file_name: str, modifications: list[list[str]]):
        # Open the file in read-only mode and read its contents into a string
        with open(f"./migrations/versions/{file_name}.py", "r") as f:
            contents = f.read()

        for modification in modifications:
            contents = contents.replace(modification[0], modification[1])

        # Open the file in write mode and write the modified contents back to the file
        with open(f"./migrations/versions/{file_name}.py", "w") as f:
            f.write(contents)

    @staticmethod
    def search(client: OpenSearch, index: str) -> dict:
        return client.search(index=index, size=1, body={},)["hits"]["hits"][
            0
        ]["_source"]
