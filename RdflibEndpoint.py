from pathlib import Path

from rdflib import Dataset
from rdflib.plugins.sparql.parser import parseUpdate

from AbstractSparqlEndpoint import AbstractSparqlEndpoint


class RdflibEndpoint(AbstractSparqlEndpoint):
    def __init__(self, endpoint: Dataset):
        super().__init__(endpoint)

    def update_query(self, query):
        qr = self.endpoint.update(query)
        return qr

    def query(self, query):
        qr = self.endpoint.query(query)
        return qr

    def import_file(self, file_path: Path, format: str) -> bool:
        self.endpoint.parse(file_path, format=format)
        return True

    def import_file_from_memory(self, file_as_str: str, format: str) -> bool:
        self.endpoint.parse(data=file_as_str, format=format)
        return True
