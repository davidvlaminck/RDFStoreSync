import datetime
import urllib
from pathlib import Path

import requests
from SPARQLWrapper import SPARQLWrapper, JSON, CSV, RDF, POST

from AbstractSparqlEndpoint import AbstractSparqlEndpoint


class GraphDbEndpoint(AbstractSparqlEndpoint):
    def query_single(self, query, param_name: str, datatype: type) -> object:
        wrapper = self._get_new_wrapper()
        wrapper.setQuery(query)
        wrapper.setReturnFormat(JSON)
        result = wrapper.queryAndConvert()
        binding_results = result['results']['bindings']
        if len(binding_results) == 0:
            return None

        # convert first result to datatype
        return datatype(binding_results[0][param_name]['value'])

    def query_single_row(self, query) -> dict:
        wrapper = self._get_new_wrapper()
        wrapper.setQuery(query)
        wrapper.setReturnFormat(JSON)
        result = wrapper.queryAndConvert()
        binding_results = result['results']['bindings']
        if len(binding_results) == 0:
            return {}
        elif len(binding_results) > 1:
            raise Exception("Query returned more than one row, rewrite the query to return only one row")
        d = binding_results[0]
        for k, v in d.items():
            datatype = v.get('datatype')
            if datatype == 'uri' or datatype is None:
                d[k] = str(v['value'])
            elif datatype == 'http://www.w3.org/2001/XMLSchema#boolean':
                if v['value'] == 'true':
                    d[k] = True
                elif v['value'] == 'false':
                    d[k] = False
                else:
                    raise Exception(f"Unknown boolean value {v['value']}")
            elif datatype == 'http://www.w3.org/2001/XMLSchema#dateTime':
                d[k] = datetime.datetime.strptime(v['value'], self.datetime_format)
            else:
                raise NotImplementedError(f"Datatype {datatype} not implemented")
        return binding_results[0]

    def __init__(self, url: str):
        super().__init__(self)
        self.url = url

    def _get_new_wrapper(self, update: bool=False) -> SPARQLWrapper:
        if update:
            sparql_wrapper = SPARQLWrapper(self.url + '/statements')
        else:
            sparql_wrapper = SPARQLWrapper(self.url)
        sparql_wrapper.setReturnFormat(JSON)
        return sparql_wrapper

    def import_file_from_memory(self, file_as_str: str, format: str) -> bool:
        raise NotImplementedError("Can't import files from memory into GraphDB")

    def import_file(self, file_path: Path, format: str) -> bool:
        url = f'{self.url}/statements'
        headers = {'Content-Type': 'application/ld+json'}

        with open(file_path, 'rb') as f:
            data = f.read()

        response = requests.post(url, headers=headers, data=data)

        if str(response.status_code)[0] != '2':
            raise Exception(f'Error importing file: {response.content.decode(encoding="utf-8")}')

        return True

    def update_query(self, query) -> object:
        wrapper = self._get_new_wrapper(update=True)
        wrapper.setQuery(query)
        wrapper.setMethod(POST)
        result = wrapper.queryAndConvert()
        return result

    def query(self, query):
        wrapper = self._get_new_wrapper()
        wrapper.setQuery(query)
        result = wrapper.queryAndConvert()
        return result



