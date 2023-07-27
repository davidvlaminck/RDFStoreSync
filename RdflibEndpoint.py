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
