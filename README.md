# opensearch-reindex

`opensearch-reindex` is a Python library that serves to help streamline reindexing data from one OpenSearch index to another.

## Features
* Migrate data from one index to another in the same cluster
* Migrate data from one index to another in different clusters
* Revision history
* Run multiple migrations one after another
* Transform documents using Python before data is inserted into destination index
* Source data is never modified or removed

## Getting started

#### 1. Install opensearch-reindex

`pip install opensearch-reindex`

or

`poetry add opensearch-reindex`

#### 2. Initialize project

`reindexer init`

#### 3. Configure your source_client and destination_client in `./migrations/env.py`

#### 4. Create `reindexer_version` index

`reindexer init-index`

This will use your `source_client` to create a new index named 'reindexer_version' and insert a new document specifying the revision version.
`{"versionNum": 0}`

When reindexing from one cluster to another, migrations should be run first (step 8) before initializing the destination cluster with:
`reindexer init-index`

#### 5. Create revision

`reindex revision 'my revision name'`

This will create a new revision file in `./migrations/versions`.

Note: revisions files should not be removed and their names should not be changed.

#### 6 Navigate to your revision file `./migrations/versions/1_my_revision_name.py` and set
`SOURCE_INDEX`, `DESTINATION_INDEX`, you can optionally set `DESTINATION_MAPPINGS`.

#### (Optional) 7. To modify documents as they are being reindexed to the destination index, update `def transform_document` accordingly

#### 8. Run your migrations
`reindexer run`

Note: When `reindexer run` is executed, it will compare revision versions in `./migrations/versions/...` to the version number in `reindexer_version` index.
All revisions that have not been run will be run one after another.
