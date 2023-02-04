# opensearch-reindexer
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/Swiple/opensearch-reindexer/graphs/commit-activity)
[![codecov](https://codecov.io/gh/Swiple/opensearch-reindexer/branch/main/graph/badge.svg?token=PA9WIZYX5Q)](https://codecov.io/gh/Swiple/opensearch-reindexer)

`opensearch-reindexer` is a Python library that serves to help streamline reindexing data from one OpenSearch 
index to another using either the native OpenSearch Reindex API or Python, the OpenSearch Scroll API and Bulk inserts.

## Features
* Native OpenSearch Reindex API and Python based reindexing using OpenSearch Scroll API
* Migrate data from one index to another in the same cluster
* Migrate data from one index to another in a different cluster
* Migration history
* Run multiple migrations one after another
* Transform documents using native OpenSearch Reindex API or Python using Scoll API and Bulk inserts
* Source indices/data is never modified or removed

## Getting started

### 1. Install opensearch-reindexer

`pip install opensearch-reindexer`

or

`poetry add opensearch-reindexer`

### 2. Initialize project

`reindexer init`

### 3. Configure your source_client in `./migrations/env.py`
You only need to configure `destination_client` if you are migrating data from one cluster to another.

### 4. Create `reindexer_version` index

`reindexer init-index`

This will use your `source_client` to create a new index named 'reindexer_version' and insert a new document specifying the revision version.
`{"versionNum": 0}`. `reindexer_version` is used to keep track of which revisions have been run.

When reindexing from one cluster to another, migrations should be run first (step 8) before initializing the destination cluster with:
`reindexer init-index`

### 5. Create revision (repeat if you have multiple indices)
Two revision types are supported, `painless` which uses the native OpenSearch Reindex API, and `python` which using
the OpenSearch Scroll API and Bulk inserts. `painless` revisions are recommended as they are more performant than 
`python` revisions. You don't have to use one or the other; `./migrations/versions/` can contain a combination of 
both `painless` and `python` revisions.

#### To create a `painless` revision run:

`reindexer revision 'my revision name'`

#### To create a `python` revision run:

`reindexer revision 'my revision name' --language python`

This will create a new revision file in `./migrations/versions`.

Note: 
1. revision files should not be removed and their names should not be changed once created.
2. `./migration/migration_template_painless.py` and `./migration/migration_template_python.py` are referenced for each revision.
You can modify them if you find yourself making the same changes to revision files.

### 6. Modify your revision file 
Navigate to your revision file `./migrations/versions/1_my_revision_name.py`

#### Painless

Modify `source` and `destination` in `REINDEX_BODY`, you can optionally set `DESTINATION_MAPPINGS`.

Note: If you only want to create the index, set the source index to `None` e.g. `"source": {"index": "reindexer_revision_1"},`

To transform data as data is reindexed, you can use 
painless scripts. For example, the following will convert data in field "c" from an object to a JSON string 
before inserting it into index `destination`.

```python
REINDEX_BODY = {
    "source": {"index": "reindexer_revision_1"},
    "dest": {"index": "reindexer_revision_2"},
    "script": {
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
    }
}
```
For more information on `REINDEX_BODY` see https://opensearch.org/docs/latest/opensearch/reindex-data/

#### Python
Modify `SOURCE_INDEX` and `DESTINATION_INDEX`, you can optionally set `DESTINATION_MAPPINGS`.

Note: If you only want to create the index, set the source index to `None` e.g. `"source": {"index": "reindexer_revision_1"},`

To modify documents as they are being re-indexed to the destination index, update `def transform_document`. For example:
```python
class Migration(BaseMigration):
    def transform_document(self, doc: dict) -> dict:
        # Modify this method to transform each document before being inserted into destination index.
        import json
        doc['c'] = json.dumps(doc['c'])
        return doc
```
### 7. See an ordered list of revisions that have not be executed
`reindexer list`

#### 8. Run your migrations
`reindexer run`

Note: When `reindexer run` is executed, it will compare revision versions in `./migrations/versions/...` to the version number in `reindexer_version` index of the source cluster.
All revisions that have not been run will be run one after another. 


## FAQ 💬 🙋 
#### How do I start using `OpenSearch reindexer` in a new project?
To get started with `OpenSearch reindexer` in a new project, simply follow the getting started guide above.

#### What happens if multiple revisions need to be executed?
`OpenSearch reindexer` compares the remote version in the `reindexer_version` index on your OpenSearch cluster to your local version. 
Any versions local revisions that have not been executed will be executed one after another.

#### What if I have multiple indices?
Create a revision for each index and follow the same steps as you would for one index.

#### How do I migrate from another schema management tool to `OpenSearch reindexer`?
In this answer, we assume that you already have 1 or many indices.

Complete steps 1 - 6 of the getting started guide, repeating steps 5 and 6 for each index. During step 6, set your source index to `None`.
This will tell `OpenSearch reindexer` to just create the index.

#### What happens when someone downloads my project that uses `OpenSearch reindexer` for the first time and revisions already exist?
If the project points to an OpenSearch cluster that has an up-to-date `reindexer_version` index, then nothing will happen the next time `reindexer run` is executed.

If the person downloading the project points to a different OpenSearch cluster that hasn't been initialized yet, they should run `reindexer init-index` to create and initialize
`reindexer_version` index followed by `reindexer run`. This will then run all migrations one after the other.
