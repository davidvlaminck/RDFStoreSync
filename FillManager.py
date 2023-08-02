import datetime
import json
from pathlib import Path

from AbstractSparqlEndpoint import AbstractSparqlEndpoint
from EMInfraImporter import EMInfraImporter
from JsonLdCompleter import JsonLdCompleter
from ResourceEnum import ResourceEnum

CURRENT_DIR = Path(__file__).parent


class FillManager:
    def __init__(self, store: AbstractSparqlEndpoint, eminfra_importer: EMInfraImporter, feed_type: ResourceEnum):
        self.feed_type = feed_type
        self.store = store
        self.eminfra_importer = eminfra_importer
        self.jsonld_completer = JsonLdCompleter()

    def fill(self, write_file_to_disk: bool = True):
        while True:
            filling_params = self._get_filling_params()
            if filling_params is not None and not bool(filling_params['state']):
                break

            fetching_cursor = None
            if filling_params is not None:
                fetching_cursor = str(filling_params['cursor'])
            response_object = self.eminfra_importer.get_objects_from_oslo_search_endpoint(
                resource=self.feed_type, cursor=fetching_cursor)

            cursor = response_object.headers.get('em-paging-next-cursor', '')
            asset_graph = response_object.graph

            # enhance asset_graph to be more Linked Data
            self.jsonld_completer.enhance_asset_graph(asset_graph)

            dt = datetime.datetime.utcnow()
            dt_str = dt.isoformat() + 'Z'

            asset_graph['@graph'].append(
                {
                    "@id": 'http://sync.params/param',
                    '@graph': [{
                        '@id': 'http://sync.params/agents',
                        'http://sync.params/filling': [
                            {
                                'http://sync.params/state': (cursor != ''),
                                'http://sync.params/cursor': cursor,
                                'http://sync.params/update_timestamp':
                                    {'@value': dt_str,
                                     '@type': 'http://www.w3.org/2001/XMLSchema#dateTime'}
                            }
                        ]
                    }]})

            assets_ld_string = json.dumps(asset_graph)

            if write_file_to_disk:
                file_path = Path(CURRENT_DIR / 'temp' / f'temp_{self.feed_type.value}.jsonld')
                with open(file_path, 'w') as f:
                    f.write(assets_ld_string)
                self.store.import_file(file_path=file_path, format='json-ld')
            else:
                self.store.import_file_from_memory(file_as_str=assets_ld_string, format='json-ld')

            if cursor == '':
                break

        # clean up temp file
        if write_file_to_disk:
            file_path = Path(CURRENT_DIR / 'temp' / f'temp_{self.feed_type.value}.jsonld')
            if Path.exists(file_path):
                file_path.unlink()

        self._clean_filling_params()

    def _clean_filling_params(self):
        # clean up triples
        # delete all triple with update_timestamp earlier than the max
        delete_q = f"""PREFIX sp:    <http://sync.params/>
        DELETE {{
            GRAPH sp:param {{
                ?bn sp:filling ?fill_node .
                ?fill_node sp:state ?state .
                ?fill_node sp:cursor ?cursor .
                ?fill_node sp:update_timestamp ?update_timestamp 
                 
            }} 
        }}
        WHERE {{ 
            {{
                SELECT (MAX(?update_timestamp) as ?max_update_timestamp)
                WHERE {{ 
                    GRAPH sp:param {{ 
                        sp:param sp:fill ?bn .
                        ?bn sp:label "{self.feed_type.value}" .
                        ?bn sp:filling ?fill_node .
                        ?fill_node sp:update_timestamp ?update_timestamp 
                    }} 
                }} 
            }}
            GRAPH sp:param {{ 
                sp:param sp:fill ?bn .
                ?bn sp:label "{self.feed_type.value}" .
                ?bn sp:filling ?fill_node .
                ?fill_node sp:state ?state .
                ?fill_node sp:cursor ?cursor .
                ?fill_node sp:update_timestamp ?update_timestamp 
            }} 
            FILTER (?update_timestamp != ?max_update_timestamp)
        }}
        """
        self.store.update_query(delete_q)

    def _get_filling_params(self) -> dict | None:
        """Fetches the filling params for the resource. Only return the params with the latest update_timestamp"""
        select_q = f"""PREFIX sp:    <http://sync.params/>
        SELECT ?state ?cursor ?max_update_timestamp 
        WHERE {{ 
            {{
                SELECT (MAX(?update_timestamp) as ?max_update_timestamp)
                WHERE {{ 
                    GRAPH sp:param {{ 
                        sp:param sp:fill ?bn .
                        ?bn sp:label "{self.feed_type.value}" .
                        ?bn sp:filling ?fill_node .
                        ?fill_node sp:update_timestamp ?update_timestamp 
                    }} 
                }} 
            }}
            GRAPH sp:param {{ 
            sp:param sp:fill ?bn .
            ?bn sp:label "{self.feed_type.value}" .
            ?bn sp:filling ?fill_node .
            ?fill_node sp:state ?state .
            ?fill_node sp:cursor ?cursor .
            ?fill_node sp:update_timestamp ?max_update_timestamp
        }} }}
        """
        result = self.store.query(select_q)
        if len(result) == 0:
            return None
        return list(result)[0].asdict()

    def print_quads(self, limit: int = 100):
        print(self.store.endpoint.serialize(format='nquads', limit=limit))
