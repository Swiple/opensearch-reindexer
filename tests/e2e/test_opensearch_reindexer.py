import json
import os
import shutil

import pytest
from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError
from opensearchpy.helpers import bulk

from opensearch_reindexer import Language, helper

REINDEXER_VERSION = "reindexer_version"
REINDEXER_SOURCE_INDEX = "reindexer_source_index"
REINDEXER_REVISION_1 = "reindexer_revision_1"
REINDEXER_REVISION_2 = "reindexer_revision_2"
REINDEXER_REVISION_3 = "reindexer_revision_3"
MODIFIED_VERSION_CONTROL_INDEX_NAME = "modified_reindexer_version"
ALIAS = "my-alias"
ALIAS_INDEX = "my-index"
ALIAS_MODIFIED_INDEX = "my-modified-index"

REVISION_ONE_MAPPINGS = {
    "mappings": {
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
}
REVISION_THREE_MAPPINGS = {
    "mappings": {
        "properties": {
            "a": {"type": "long"},
            "c": {"type": "text"},
        }
    }
}


@pytest.fixture()
def clean_up():
    source_client = get_os_client()
    delete_index(source_client, REINDEXER_VERSION)
    delete_index(source_client, REINDEXER_SOURCE_INDEX)
    delete_index(source_client, REINDEXER_REVISION_1)
    delete_index(source_client, REINDEXER_REVISION_2)
    delete_index(source_client, REINDEXER_REVISION_3)
    delete_index(source_client, MODIFIED_VERSION_CONTROL_INDEX_NAME)
    delete_index(source_client, ALIAS_INDEX)
    delete_index(source_client, ALIAS_MODIFIED_INDEX)
    source_client.indices.delete_alias(name=ALIAS, index=ALIAS_INDEX, ignore=[404])
    source_client.indices.delete_alias(
        name=ALIAS, index=ALIAS_MODIFIED_INDEX, ignore=[404]
    )

    if os.path.exists("./migrations"):
        shutil.rmtree("./migrations")


@pytest.fixture()
def load_data():
    source_client = get_os_client()
    load_reindexer_source_index(source_client, REINDEXER_SOURCE_INDEX)


class TestOpensearchReindexerHelper:
    def test_create_or_update_alias_creates_alias(self, clean_up):
        # Test that the function creates an alias if it doesn't exist
        client = get_os_client()
        client.indices.create(index=ALIAS_INDEX)
        helper.create_or_update_alias(client, ALIAS, ALIAS_INDEX)
        aliases = json.dumps(client.indices.get_alias(name=ALIAS))
        assert ALIAS in aliases
        assert ALIAS_INDEX in aliases

    def test_create_or_update_alias_updates_alias(self, clean_up):
        # Test that the function updates an existing alias
        client = get_os_client()
        client.indices.create(index=ALIAS_INDEX)
        helper.create_or_update_alias(client, ALIAS, ALIAS_INDEX)

        client.indices.create(index=ALIAS_MODIFIED_INDEX)
        helper.create_or_update_alias(client, ALIAS, ALIAS_MODIFIED_INDEX)
        aliases = json.dumps(client.indices.get_alias(name=ALIAS))

        assert ALIAS in aliases
        assert ALIAS_INDEX not in aliases
        assert ALIAS_MODIFIED_INDEX in aliases

    def test_create_or_update_alias_returns_none(self, clean_up):
        # Test that the function returns None
        client = get_os_client()
        client.indices.create(index=ALIAS_INDEX)
        result = helper.create_or_update_alias(client, ALIAS, ALIAS_INDEX)
        assert result is None

    def test_should_increment_index(self, clean_up):
        inputs = [
            "my-index",
            "my-index-0",
            "my-index-longer",
            "my-index-9",
            "my-index-",
            "-my-index",
        ]
        outputs = [
            "my-index-0",
            "my-index-1",
            "my-index-longer-0",
            "my-index-10",
            "my-index--0",
            "-my-index-0",
        ]

        for i, val in enumerate(inputs):
            assert helper.increment_index(val) == outputs[i]


class TestOpensearchReindexer:
    def test_should_show_prerequisite_steps_to_reindexer_list(self, clean_up):
        import opensearch_reindexer as osr

        with pytest.raises(SystemExit) as excinfo:
            osr.list()
        # assert that the correct exit code was used
        assert excinfo.value.code == 1

        osr.init()
        with pytest.raises(SystemExit) as excinfo:
            osr.list()
        # assert that the correct exit code was used
        assert excinfo.value.code == 1

        osr.init_index()
        with pytest.raises(SystemExit) as excinfo:
            osr.list()
        # assert that the correct exit code was used
        assert excinfo.value.code == 1

        osr.revision("revision_1")

        osr.list()

    def test_should_show_prerequisite_steps_to_reindexer_init_index(self, clean_up):
        import opensearch_reindexer as osr

        with pytest.raises(SystemExit) as excinfo:
            osr.init_index()

        # assert that the correct exit code was used
        assert excinfo.value.code == 1

    def test_init_index_has_already_been_run(self, clean_up):
        import opensearch_reindexer as osr

        osr.init()
        osr.init_index()
        with pytest.raises(SystemExit) as excinfo:
            osr.init_index()

        # assert that the correct exit code was used
        assert excinfo.value.code == 0

    def test_should_show_prerequisite_steps_to_reindexer_run(self, clean_up):
        import opensearch_reindexer as osr

        with pytest.raises(SystemExit) as excinfo:
            osr.run()

        # assert that the correct exit code was used
        assert excinfo.value.code == 1

    def test_should_show_prerequisite_steps_to_reindexer_run_two(self, clean_up):
        import opensearch_reindexer as osr

        source_client = get_os_client()

        osr.init()
        osr.init_index()
        osr.revision("revision_1")
        delete_index(source_client, REINDEXER_VERSION)
        with pytest.raises(SystemExit) as excinfo:
            osr.run()

        # assert that the correct exit code was used
        assert excinfo.value.code == 1

    def test_no_new_revisions_found(self, clean_up, load_data):
        import opensearch_reindexer as osr

        osr.init()
        osr.init_index()
        osr.revision("revision_1")

        reindex_body_source = '"source": {"index": "source"},'
        reindex_body_destination = '"dest": {"index": "destination"},'

        modify_revision_file(
            file_name="1_revision_1",
            modifications=[
                [
                    reindex_body_source,
                    f'"source": {{"index": "{REINDEXER_SOURCE_INDEX}"}},',
                ],
                [
                    reindex_body_destination,
                    f'"dest": {{"index": "{REINDEXER_REVISION_1}"}},',
                ],
            ],
        )

        osr.run()

        with pytest.raises(SystemExit) as excinfo:
            osr.run()

        # assert that the correct exit code was used
        assert excinfo.value.code == 0

    def test_should_not_allow_invalid_revisions_file_names(self, clean_up):
        import opensearch_reindexer as osr

        osr.init()
        osr.init_index()
        with pytest.raises(SystemExit) as excinfo:
            osr.revision("invalid.revision name")

        # assert that the correct exit code was used
        assert excinfo.value.code == 1

    def test_should_exit_if_language_is_not_supported(self, clean_up):
        import opensearch_reindexer as osr

        osr.init()
        osr.init_index()
        with pytest.raises(SystemExit) as excinfo:
            osr.revision("revision_1", "unsupported_language")

        # assert that the correct exit code was used
        assert excinfo.value.code == 1

    def test_revisions_should_exist_before_running_reindexer_run(self, clean_up):
        import opensearch_reindexer as osr

        osr.init()
        osr.init_index()

        with pytest.raises(SystemExit) as excinfo:
            osr.run()

        # assert that the correct exit code was used
        assert excinfo.value.code == 1

    def test_should_update_reindexer_version_in_source_and_destination_cluster_when_hosts_are_different(
        self, clean_up, load_data
    ):
        import opensearch_reindexer as osr

        source_client = get_os_client()

        osr.init()

        # verify that "reindexer_version" index is initialized to version 0 when no revisions exist
        osr.init_index()
        assert search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 0}

        # create revisions
        osr.revision("revision_1", str(Language.python.value))
        osr.revision("revision_2", str(Language.python.value))
        osr.revision("revision_3", str(Language.python.value))

        # modify revision files source/destination indices, mappings and transform_document
        modify_revision_files_python()
        modify_destination_client_env_file()

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
        assert search(
            client=source_client,
            index=REINDEXER_REVISION_1,
        ) == {"a": 1, "b": 1, "c": {"a": "a", "b": "b", "c": 2}}

        assert search(
            client=source_client,
            index=REINDEXER_REVISION_2,
        ) == {"a": 1, "b": 1, "c": json.dumps({"a": "a", "b": "b", "c": 2})}

        assert search(
            client=source_client,
            index=REINDEXER_REVISION_3,
        ) == {"a": 1, "c": json.dumps({"a": "a", "b": "b", "c": 2})}

    def test_should_use_different_version_control_index_name(self, clean_up, load_data):
        import opensearch_reindexer as osr

        source_client = get_os_client()

        osr.init()
        modify_version_control_index()

        # verify that "reindexer_version" index is initialized to version 0 when no revisions exist
        osr.init_index()
        assert search(
            client=source_client,
            index=MODIFIED_VERSION_CONTROL_INDEX_NAME,
        ) == {"versionNum": 0}

        # create revisions
        osr.revision("revision_1", str(Language.python.value))
        osr.revision("revision_2", str(Language.python.value))
        osr.revision("revision_3", str(Language.python.value))

        # modify revision files source/destination indices, mappings and transform_document
        modify_revision_files_python()

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
        assert search(
            client=source_client,
            index=REINDEXER_REVISION_1,
        ) == {"a": 1, "b": 1, "c": {"a": "a", "b": "b", "c": 2}}

        assert search(
            client=source_client,
            index=REINDEXER_REVISION_2,
        ) == {"a": 1, "b": 1, "c": json.dumps({"a": "a", "b": "b", "c": 2})}

        assert search(
            client=source_client,
            index=REINDEXER_REVISION_3,
        ) == {"a": 1, "c": json.dumps({"a": "a", "b": "b", "c": 2})}

        assert search(
            client=source_client,
            index=MODIFIED_VERSION_CONTROL_INDEX_NAME,
        ) == {"versionNum": 3}

    def test_should_exit_when_source_index_does_not_exist_python(self, clean_up):
        import opensearch_reindexer as osr

        source_client = get_os_client()

        osr.init()

        # verify that "reindexer_version" index is initialized to version 0 when no revisions exist
        osr.init_index()
        assert search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 0}

        # create revisions
        osr.revision("revision_1", str(Language.python.value))

        non_existent_index = "non_existent_index"
        modify_revision_file(
            file_name="1_revision_1",
            modifications=[
                ['SOURCE_INDEX = ""', f"SOURCE_INDEX = '{non_existent_index}'"],
                [
                    'DESTINATION_INDEX = ""',
                    f"DESTINATION_INDEX = '{REINDEXER_REVISION_1}'",
                ],
            ],
        )

        with pytest.raises(SystemExit) as excinfo:
            osr.run()

        # assert that the correct exit code was used
        assert excinfo.value.code == 1
        assert source_client.indices.exists(index=non_existent_index) is False
        assert source_client.indices.exists(index=REINDEXER_REVISION_1) is False

    def test_should_exit_when_source_index_does_not_exist_painless(self, clean_up):
        import opensearch_reindexer as osr

        source_client = get_os_client()

        osr.init()

        # verify that "reindexer_version" index is initialized to version 0 when no revisions exist
        osr.init_index()
        assert search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 0}

        # create revisions
        osr.revision("revision_1")

        non_existent_index = "non_existent_index"
        modify_revision_file(
            file_name="1_revision_1",
            modifications=[
                [
                    '"source": {"index": "source"},',
                    f'"source": {{"index": "{non_existent_index}"}},',
                ],
                [
                    '"dest": {"index": "destination"},',
                    f'"dest": {{"index": "{REINDEXER_REVISION_1}"}},',
                ],
            ],
        )

        with pytest.raises(SystemExit) as excinfo:
            osr.run()

        # assert that the correct exit code was used
        assert excinfo.value.code == 1
        assert source_client.indices.exists(index=non_existent_index) is False
        assert source_client.indices.exists(index=REINDEXER_REVISION_1) is False

    def test_should_create_destination_index_and_not_reindex_when_source_index_is_none_python(
        self, clean_up
    ):
        import opensearch_reindexer as osr

        source_client = get_os_client()

        osr.init()

        # verify that "reindexer_version" index is initialized to version 0 when no revisions exist
        osr.init_index()
        assert search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 0}

        # create revisions
        osr.revision("revision_1", str(Language.python.value))

        modify_revision_file(
            file_name="1_revision_1",
            modifications=[
                ['SOURCE_INDEX = ""', f"SOURCE_INDEX = None"],
                [
                    'DESTINATION_INDEX = ""',
                    f"DESTINATION_INDEX = '{REINDEXER_REVISION_1}'",
                ],
                [
                    "DESTINATION_INDEX_BODY = None",
                    f"DESTINATION_INDEX_BODY = {REVISION_ONE_MAPPINGS}",
                ],
            ],
        )

        osr.run()

        # Expect destination index to be created
        assert source_client.indices.exists(REINDEXER_REVISION_1) is True

        # No reindexing should have occurred because source index is None
        assert source_client.count(index=REINDEXER_REVISION_1)["count"] == 0
        assert (
            source_client.indices.get_mapping(index=REINDEXER_REVISION_1)[
                REINDEXER_REVISION_1
            ]
            == REVISION_ONE_MAPPINGS
        )

        # versionNum should be incremented if source index is None and destination index does not exist.
        assert search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 1}

    def test_should_create_destination_index_and_not_reindex_when_source_index_is_none_painless(
        self, clean_up
    ):
        import opensearch_reindexer as osr

        source_client = get_os_client()

        osr.init()

        # verify that "reindexer_version" index is initialized to version 0 when no revisions exist
        osr.init_index()
        assert search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 0}

        # create revisions
        osr.revision("revision_1")

        modify_revision_file(
            file_name="1_revision_1",
            modifications=[
                [
                    '"source": {"index": "source"},',
                    f'"source": {{"index": None}},',
                ],
                [
                    '"dest": {"index": "destination"},',
                    f'"dest": {{"index": "{REINDEXER_REVISION_1}"}},',
                ],
                [
                    "DESTINATION_INDEX_BODY = None",
                    f"DESTINATION_INDEX_BODY = {REVISION_ONE_MAPPINGS}",
                ],
            ],
        )
        osr.run()

        # Expect destination index to be created
        assert source_client.indices.exists(REINDEXER_REVISION_1) is True

        # No reindexing should have occurred because source index is None
        assert source_client.count(index=REINDEXER_REVISION_1)["count"] == 0
        assert (
            source_client.indices.get_mapping(index=REINDEXER_REVISION_1)[
                REINDEXER_REVISION_1
            ]
            == REVISION_ONE_MAPPINGS
        )

        # versionNum should be incremented if source index is None and destination index does not exist.
        assert search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 1}

    def test_should_invoke_hooks_in_order(self, clean_up, load_data):
        import opensearch_reindexer as osr

        osr.init()
        osr.init_index()

        osr.revision("revision_1")
        reindex_body_source = '"source": {"index": "source"},'
        reindex_body_destination = '"dest": {"index": "destination"},'

        modify_revision_file(
            file_name="1_revision_1",
            modifications=[
                [
                    reindex_body_source,
                    f'"source": {{"index": "{REINDEXER_SOURCE_INDEX}"}},',
                ],
                [
                    reindex_body_destination,
                    f'"dest": {{"index": "{REINDEXER_REVISION_1}"}},',
                ],
                [
                    "DESTINATION_INDEX_BODY = None",
                    f"DESTINATION_INDEX_BODY = {REVISION_ONE_MAPPINGS}",
                ],
            ],
        )

        string_to_modify = """class Migration(BaseMigration):
    def before_revision(self):
        pass

    def after_revision(self):
        pass"""

        replacement_string = """class Migration(BaseMigration):
    def before_revision(self):
        alias_name = "my_alias"
        index_name = self.config.source_index

        # Create the alias
        self.destination_client.indices.put_alias(index=index_name, name=alias_name)

    def after_revision(self):
        alias_name = "my_alias"

        # Verify that the alias was created
        aliases = self.destination_client.indices.get_alias(name=alias_name)
        assert self.config.source_index in aliases

        # Update the alias to point to the new index
        self.destination_client.indices.update_aliases(body={
            "actions": [
                {
                    "remove": {
                        "index": self.config.source_index,
                        "alias": alias_name
                    }
                },
                {
                    "add": {
                        "index": self.config.destination_index,
                        "alias": alias_name
                    }
                }
            ]
        })
"""
        modify_revision_file(
            file_name="1_revision_1",
            modifications=[
                [string_to_modify, replacement_string],
            ],
        )

        osr.run()

        source_client = get_os_client()

        aliases = source_client.indices.get_alias(name="my_alias")
        assert REINDEXER_REVISION_1 in aliases

    def test_setup_and_run_revisions_python(self, clean_up, load_data):
        import opensearch_reindexer as osr

        source_client = get_os_client()

        osr.init()

        # verify that "reindexer_version" index is initialized to version 0 when no revisions exist
        osr.init_index()
        assert search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 0}

        # create revisions
        osr.revision("revision_1", str(Language.python.value))
        osr.revision("revision_2", str(Language.python.value))
        osr.revision("revision_3", str(Language.python.value))

        # modify revision files source/destination indices, mappings and transform_document
        modify_revision_files_python()

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
        assert search(
            client=source_client,
            index=REINDEXER_REVISION_1,
        ) == {"a": 1, "b": 1, "c": {"a": "a", "b": "b", "c": 2}}

        assert search(
            client=source_client,
            index=REINDEXER_REVISION_2,
        ) == {"a": 1, "b": 1, "c": json.dumps({"a": "a", "b": "b", "c": 2})}

        assert search(
            client=source_client,
            index=REINDEXER_REVISION_3,
        ) == {"a": 1, "c": json.dumps({"a": "a", "b": "b", "c": 2})}

    def test_setup_and_run_revisions_painless(self, clean_up, load_data):
        import opensearch_reindexer as osr

        source_client = get_os_client()

        osr.init()

        # verify that "reindexer_version" index is initialized to version 0 when no revisions exist
        osr.init_index()
        assert search(
            client=source_client,
            index=REINDEXER_VERSION,
        ) == {"versionNum": 0}

        # create revisions
        osr.revision("revision_1")
        osr.revision("revision_2", str(Language.painless.value))
        osr.revision("revision_3")

        # modify revision files source/destination indices, mappings and transform_document
        modify_revision_files_painless()

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
        assert search(
            client=source_client,
            index=REINDEXER_REVISION_1,
        ) == {"a": 1, "b": 1, "c": {"a": "a", "b": "b", "c": 2}}

        assert search(
            client=source_client,
            index=REINDEXER_REVISION_2,
        ) == {"a": 1, "b": 1, "c": '{"a":"a","b":"b","c":"2"}'}

        assert search(
            client=source_client,
            index=REINDEXER_REVISION_3,
        ) == {"a": 1, "c": '{"a":"a","b":"b","c":"2"}'}


def modify_revision_files_python():
    modify_revision_file(
        file_name="1_revision_1",
        modifications=[
            ['SOURCE_INDEX = ""', f"SOURCE_INDEX = '{REINDEXER_SOURCE_INDEX}'"],
            [
                'DESTINATION_INDEX = ""',
                f"DESTINATION_INDEX = '{REINDEXER_REVISION_1}'",
            ],
            [
                "DESTINATION_INDEX_BODY = None",
                f"DESTINATION_INDEX_BODY = {REVISION_ONE_MAPPINGS}",
            ],
        ],
    )

    revision_two_code = """
            import json
            doc['c'] = json.dumps(doc['c'])
            return doc
            """
    modify_revision_file(
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
    modify_revision_file(
        file_name="3_revision_3",
        modifications=[
            ['SOURCE_INDEX = ""', f"SOURCE_INDEX = '{REINDEXER_REVISION_2}'"],
            [
                'DESTINATION_INDEX = ""',
                f"DESTINATION_INDEX = '{REINDEXER_REVISION_3}'",
            ],
            [
                "DESTINATION_INDEX_BODY = None",
                f"DESTINATION_INDEX_BODY = {REVISION_THREE_MAPPINGS}",
            ],
            ["return doc", revision_three_code],
        ],
    )


def modify_revision_files_painless():
    reindex_body_source = '"source": {"index": "source"},'
    reindex_body_destination = '"dest": {"index": "destination"},'

    modify_revision_file(
        file_name="1_revision_1",
        modifications=[
            [
                reindex_body_source,
                f'"source": {{"index": "{REINDEXER_SOURCE_INDEX}"}},',
            ],
            [
                reindex_body_destination,
                f'"dest": {{"index": "{REINDEXER_REVISION_1}"}},',
            ],
            [
                "DESTINATION_INDEX_BODY = None",
                f"DESTINATION_INDEX_BODY = {REVISION_ONE_MAPPINGS}",
            ],
        ],
    )

    painless_script = '''"script": {
"lang": "painless",
"source": """
def jsonString = '{';
int counter = 1;
int size = ctx._source.c.size();
for (def entry : ctx._source.c.entrySet()) {
  jsonString += '"'+entry.getKey()+'":'+'"'+entry.getValue()+'"';
  if (counter != size) {
    jsonString += ',';
  }
  counter++;
}
jsonString += '}';
ctx._source.c = jsonString;
"""
}'''
    modify_revision_file(
        file_name="2_revision_2",
        modifications=[
            [reindex_body_source, f'"source": {{"index": "{REINDEXER_REVISION_1}"}},'],
            [
                reindex_body_destination,
                f'"dest": {{"index": "{REINDEXER_REVISION_2}"}},\n{painless_script}',
            ],
        ],
    )

    painless_script = str(
        {"script": {"lang": "painless", "source": "ctx._source.remove('b')"}}
    )
    modify_revision_file(
        file_name="3_revision_3",
        modifications=[
            [reindex_body_source, f'"source": {{"index": "{REINDEXER_REVISION_2}"}},'],
            [
                reindex_body_destination,
                f'"dest": {{"index": "{REINDEXER_REVISION_3}"}},\n{painless_script[1:-1]}',
            ],
            [
                "DESTINATION_INDEX_BODY = None",
                f"DESTINATION_INDEX_BODY = {REVISION_THREE_MAPPINGS}",
            ],
        ],
    )


def get_os_client():
    return OpenSearch(
        hosts=[{"host": "localhost", "port": 9200}],
        http_compress=True,
        http_auth=("admin", "admin"),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False,
    )


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


def delete_index(client, index):
    try:
        client.indices.delete(index)
    except NotFoundError:
        pass


def modify_revision_file(file_name: str, modifications: list[list[str]]):
    # Open the file in read-only mode and read its contents into a string
    with open(f"./migrations/versions/{file_name}.py", "r") as f:
        contents = f.read()

    for modification in modifications:
        contents = contents.replace(modification[0], modification[1])

    # Open the file in write mode and write the modified contents back to the file
    with open(f"./migrations/versions/{file_name}.py", "w") as f:
        f.write(contents)


def modify_destination_client_env_file():
    # Open the file in read-only mode and read its contents into a string
    with open(f"./migrations/env.py", "r") as f:
        contents = f.read()
        destination_client = """destination_client = OpenSearch(
    hosts=[{"host": "127.0.0.1", "port": OPENSEARCH_PORT}],
    http_compress=True,  # enables gzip compression for request bodies
    http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
    use_ssl=OPENSEARCH_USE_SSL,
    verify_certs=OPENSEARCH_VERIFY_CERTS,
    ssl_show_warn=False,
)
        """
        contents = contents.replace(
            "destination_client = source_client", destination_client
        )

    # Open the file in write mode and write the modified contents back to the file
    with open(f"./migrations/env.py", "w") as f:
        f.write(contents)


def modify_version_control_index():
    # Open the file in read-only mode and read its contents into a string
    with open(f"./migrations/env.py", "r") as f:
        contents = f.read()
        version_control_index = (
            f'VERSION_CONTROL_INDEX = "{MODIFIED_VERSION_CONTROL_INDEX_NAME}"'
        )
        contents = contents.replace(
            'VERSION_CONTROL_INDEX = "reindexer_version"', version_control_index
        )

    # Open the file in write mode and write the modified contents back to the file
    with open(f"./migrations/env.py", "w") as f:
        f.write(contents)


def search(client: OpenSearch, index: str) -> dict:
    return client.search(index=index, size=1, body={})["hits"]["hits"][0]["_source"]
