import json
from weaviate import Client


def create_schema(client: Client, class_obj: dict) -> str:
    client.schema.delete_all()
    client.schema.create_class(class_obj)
    schema = client.schema.get()
    return json.dumps(schema, indent=4)
