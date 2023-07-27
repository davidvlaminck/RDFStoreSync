import urllib

import requests

from AbstractSparqlEndpoint import AbstractSparqlEndpoint


class GraphDbEndpoint(AbstractSparqlEndpoint):
    def __init__(self, endpoint):
        super().__init__(endpoint)

    def query(self, query):
        params = {"query": query}
        encoded = urllib.parse.urlencode(params)
        url = f'{self.endpoint}?{encoded}'
        headers = {"Content-type": "application/sparql-query"}
        print(url)
        r = requests.post(url, headers=headers, data=params['query'])
        print(f'status code: {r.status_code}')
        print(r.content.decode(encoding='utf-8'))
        return r.content.decode(encoding='utf-8')
