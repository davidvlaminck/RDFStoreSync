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

    def query_single(self, query, param_name: str, datatype: type) -> object:
        qr = self.endpoint.query(query)
        qr_list = list(qr)
        if len(qr_list) == 0:
            return None
        return datatype(qr_list[0][0])

    def query_single_row(self, query) -> dict:
        qr = self.endpoint.query(query)
        qr_list = list(qr)
        if len(qr_list) == 0:
            return {}
        d = qr_list[0].asdict()
        for k, v in d.items():
            d[k] = v.toPython()
        return d

    def import_file(self, file_path: Path, format: str) -> bool:
        self.endpoint.parse(file_path, format=format)
        return True

    def import_file_from_memory(self, file_as_str: str, format: str) -> bool:
        self.endpoint.parse(data=file_as_str, format=format)
        return True


