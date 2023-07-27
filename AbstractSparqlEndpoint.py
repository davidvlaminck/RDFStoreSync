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
        self.endpoint = endpoint

    def query(self, query):
        """
        Interface for querying the SPARQL endpoint.

        @param query: The SPARQL query.
        @type query: string

        @return: The SPARQL query result.
        """
        raise NotImplementedError("AbstractSparqlEndpoint.query()")

    def count_triples(self, graph_uri: str):
        """
        Count the triples in the SPARQL endpoint given the named graph.

        @return: The number of triples.
        @rtype: int
        """
        result = self.query("""SELECT (COUNT(?s) AS ?triples) { GRAPH <""" + graph_uri + """> { ?s ?p ?o } }""")
        r = int(list(result)[0][0])
        return r


    def clear_all(self):
        """
        Clear all triples in the SPARQL endpoint.

        @return: The SPARQL query result.
        """
        result = self.query("""CLEAR ALL""")
        return result
