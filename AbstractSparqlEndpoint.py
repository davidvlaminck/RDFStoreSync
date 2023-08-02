import abc
import datetime
from pathlib import Path


class AbstractSparqlEndpoint(object):
    """
    Abstract class for SPARQL endpoints.
    """

    def __init__(self, endpoint: object):
        """
        Initialize the SPARQL endpoint.

        @param endpoint: The SPARQL endpoint.
        @type endpoint: string
        """
        self.datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        self.endpoint = endpoint


    @abc.abstractmethod
    def update_query(self, query) -> object:
        """
        Interface for querying the SPARQL endpoint. This is for update queries.

        @param query: The SPARQL query.
        @type query: string

        @return: The SPARQL query result.
        """
        raise NotImplementedError("AbstractSparqlEndpoint.query()")

    @abc.abstractmethod
    def query_single_row(self, query) -> dict:
        """
        Interface for querying the SPARQL endpoint. This is for a non-update query that will return a single row.
        The single row will be a dictionary.

        @param query: The SPARQL query.
        @type query: string
        @param datatype: The datatype of the result.
        @type datatype: type

        @return: The SPARQL query result.

        """
        raise NotImplementedError("AbstractSparqlEndpoint.query_single_row()")

    @abc.abstractmethod
    def query_single(self, query, param_name: str, datatype: type) -> object:
        """
        Interface for querying the SPARQL endpoint. This is for non-update queries and single results.

        @param query: The SPARQL query.
        @type query: string
        @param datatype: The datatype of the result.
        @type datatype: type

        @return: The SPARQL query result.

        """
        raise NotImplementedError("AbstractSparqlEndpoint.query_single()")

    @abc.abstractmethod
    def query(self, query) -> object:
        """
        Interface for querying the SPARQL endpoint. This is for non-update queries.

        @param query: The SPARQL query.
        @type query: string

        @return: The SPARQL query result.
        """
        raise NotImplementedError("AbstractSparqlEndpoint.query()")

    def count_triples(self, graph_uri: str) -> int:
        """
        Count the triples in the SPARQL endpoint given the named graph.

        @return: The number of triples.
        @rtype: int
        """
        result = self.query("""SELECT (COUNT(?s) AS ?triples) { GRAPH <""" + graph_uri + """> { ?s ?p ?o } }""")
        r = int(list(result)[0][0])
        return r

    def count_named_graphs(self) -> int:
        """
        Count the number of named graphs in the SPARQL endpoint

        @return: The number of named graphs.
        @rtype: int
        """
        result = self.query("""SELECT (COUNT(?graph) AS ?named_graphs) { GRAPH ?graph { ?s ?p ?o } }""")
        r = int(list(result)[0][0])
        return r

    def clear_all(self):
        """
        Clear all triples in the SPARQL endpoint.

        @return: The SPARQL query result.
        """
        result = self.query("""CLEAR ALL""")
        return result

    @abc.abstractmethod
    def import_file(self, file_path: Path, format: str) -> bool:
        """
        Import a file into the SPARQL endpoint.

        @param file_path: The file path.
        @type file_path: Path
        @param format: The file format.
        @type format: str

        @return: The SPARQL query result.
        """
        raise NotImplementedError("AbstractSparqlEndpoint.import_file()")

    @abc.abstractmethod
    def import_file_from_memory(self, file_as_str: str, format: str) -> bool:
        """
        Imports the contents of a file in memory into the SPARQL endpoint.

        @param file_as_str: The file as string.
        @type file_as_str: str
        @param format: The file format.
        @type format: str

        @return: The SPARQL query result.
        """
        raise NotImplementedError("AbstractSparqlEndpoint.import_file_from_memory()")
