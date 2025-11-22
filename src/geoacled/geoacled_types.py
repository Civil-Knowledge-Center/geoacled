from typing import Any, TypedDict


class Feature(TypedDict):
    properties: dict[str, Any]

class FeatureCollection(TypedDict):
    features: list[Feature]