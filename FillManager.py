import datetime
import json
from pathlib import Path

from rdflib import Dataset

from AbstractSparqlEndpoint import AbstractSparqlEndpoint
from EMInfraImporter import EMInfraImporter
from ResourceEnum import ResourceEnum

CURRENT_DIR = Path(__file__).parent


class FillManager:
    def __init__(self, store: AbstractSparqlEndpoint, eminfra_importer: EMInfraImporter, feed_type: ResourceEnum):
        self.feed_type = feed_type
        self.store = store
        self.eminfra_importer = eminfra_importer

    def fill(self):
        while True:
            filling_params = self._get_filling_params()
            if filling_params is not None and not filling_params['state']:
                break

            response_object = self.eminfra_importer.get_objects_from_oslo_search_endpoint(resource=self.feed_type)
            cursor = response_object.headers['em-paging-next-cursor']
            asset_graph = response_object.graph
            asset_graph = {'@graph': []}
            dt = datetime.datetime.utcnow()
            dt_str = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

            asset_graph['@graph'].append(
                {
                    "@id": 'http://sync.params/param',
                    '@graph': [{
                    '@id': 'http://sync.params/agents',
                    'http://sync.params/filling': [
                        {
                            'http://sync.params/state': True,
                            'http://sync.params/cursor': cursor,
                            'http://sync.params/update_datum': dt_str, # add type
                        }
                    ]
                }]})

            assets_ld_string = json.dumps(asset_graph)
            this_directory = CURRENT_DIR
            file_path = Path(this_directory / 'temp' / f'temp_{self.feed_type.value}.jsonld')
            with open(file_path, 'w') as f:
                f.write(assets_ld_string)

            #ds = Dataset()
            #ds.parse(location=file_path, format='json-ld')

            self.store.endpoint.parse(data=assets_ld_string, format='json-ld')

            self.print_quads()
            break

            # save jsonld to disk
            # import jsonld file into store

        # assets (fill) > filling > (bnode) > filling_state > bool
        # check if the boolean exists
        # if not, create it
        # if yes, check if it is true, if so, continue
        # if false, stop

        # stop when all filling booleans are false
        # if all filling booleans are true, set step to 3

        # dt : "2005-01-01T00:00:00Z"^^xsd:dateTime

    def _get_filling_params(self) -> dict | None:
        select_q = f"""PREFIX sp:    <http://sync.params/>
        SELECT ?filling_state ?cursor ?update_datum WHERE {{ GRAPH sp:param {{ 
            sp:param sp:fill ?bn .
            ?bn sp:label "{self.feed_type.value}" .
            ?bn sp:filling ?fill_node .
            ?fill_node sp:state ?state .
            ?fill_node sp:cursor ?cursor .
            ?fill_node sp:update_datum ?update_datum
        }} }}"""
        result = self.store.query(select_q)
        if len(result) == 0:
            return None
        return list(result)[0].asdict()

    def print_quads(self, limit: int = 100):
        print(self.store.endpoint.serialize(format='nquads', limit=limit))