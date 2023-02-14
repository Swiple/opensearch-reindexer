import json

from opensearchpy import OpenSearch


def create_or_update_alias(client: OpenSearch, alias: str, index_name: str) -> None:
    """Create or update an alias in an OpenSearch index. If the alias already exists, remove any
    existing indices associated with the alias and update it to point to the specified index.

    Arguments:
        client (OpenSearch): An instance of the `OpenSearch` class representing a connection to an OpenSearch service.
        alias (str): The name of the alias to be created or updated.
        index_name (str): The name of the index to which the alias should point.

    Returns:
        None
    """
    if not client.indices.exists_alias(name=alias):
        client.indices.put_alias(name=alias, index=index_name)
    else:
        client.indices.update_aliases(
            body={
                "actions": [
                    {
                        "remove": {"index": i, "alias": alias}
                        for i in client.indices.get_alias(name=alias)
                    },
                    {"add": {"index": index_name, "alias": alias}},
                ]
            }
        )


def increment_index(index_name) -> str:
    """Increment the suffix of an index name.

    Arguments:
        index_name (str): The index name to be incremented.

    Returns:
        str: The incremented index name.
    """
    try:
        suffix = int(index_name.split("-")[-1])
        return "-".join(index_name.split("-")[:-1]) + "-" + str(suffix + 1)
    except ValueError:
        return index_name + "-0"
