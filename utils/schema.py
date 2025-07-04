"""
Handles the schema response and parses it
"""
import pprint
from datetime import datetime


class Attribute:
    def __init__(self, name, primary_key, foreign_key, type):
        self.name = name
        self.primary_key = primary_key
        self.foreign_key = foreign_key
        self.type = type


class Resource:
    def __init__(self, name, attributes, allowed_query_modes):
        self.name = name
        self.attributes: list[Attribute] = attributes
        self.allowed_query_modes: list[str] = allowed_query_modes
        self.last_change: datetime = None


class Schema:
    def __init__(self, resources):
        self.resources: list[Resource] = resources
        self.lookup: dict[dict] = self.__create_resource_column_type_map()
        #self.lookup: dict[dict] = create_resource_column_type_map(resources)

    def __create_resource_column_type_map(self):
        mdict = {}
        for r in self.resources:
            mdict[r.name] = {}
            for a in r.attributes:
                mdict[r.name][a.name] = a.type
        return mdict


def create_resource_column_type_map(resources: list[Resource]) -> dict[dict]:
    mdict = {}
    for r in resources:
        mdict[r.name] = {}
        for a in r.attributes:
            mdict[r.name][a.name] = a.type
    return mdict


def parse_schema_resources(data: dict) -> Schema:

    # pprint.pprint(data)

    resources: list[Resource] = []
    b = data["resources"]

    # collect the resource names
    for name in b:
        # print(name)
        attributes: list[Attribute] = []
        for attr in b[name]["attributes"]:
            # print(f"attr: {attr}")
            for k, v in attr.items():
                attributes.append(Attribute(k, v["primary_key"],
                                            v["foreign_key"],
                                            v["type"],
                                            )
                                  )

        resources.append(Resource(name, attributes, b[name]["allowedQueryModes"]))
    return Schema(resources=resources)