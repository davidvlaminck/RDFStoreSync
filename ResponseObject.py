from dataclasses import dataclass


@dataclass
class ResponseObject:
    graph: dict
    headers: dict