import concurrent
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from EMInfraImporter import EMInfraImporter
from FillManager import FillManager
from ResourceEnum import ResourceEnum


class StoreSyncer:
    def __init__(self, store, eminfra_importer: EMInfraImporter):
        self.store = store
        self.eminfra_importer: EMInfraImporter = eminfra_importer

    def run(self):
        """
        Run the syncer.
        """
        while True:
            if not self.check_for_sync_params():
                self._init_store()

            step = self.get_step()
            if step == 0:
                self._reset_store()
            elif step == 1:
                self._perform_init_store()
            elif step == 2:
                self._perform_filling()
            elif step == 3:
                raise NotImplementedError('step 3 not implemented')

    def check_for_sync_params(self) -> bool:
        """
        Check if the store has the sync parameters.
        """
        result = self.store.query("""ASK { GRAPH <http://sync.params/param> { ?s ?p ?o }}""")
        return bool(list(result)[0])

    def _init_store(self):
        insert_query = """
PREFIX sp:  <http://sync.params/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
INSERT DATA { 
    GRAPH sp:param {
        sp:param sp:step "1"^^xsd:integer
    }
};
"""
        self.store.update_query(insert_query)

    def print_quads(self, limit: int = 100):
        print(self.store.endpoint.serialize(format='nquads', limit=limit))

    def _reset_store(self):
        drop_query = """CLEAR ALL;"""
        self.store.update_query(drop_query)
        self._init_store()

    def reset_store_insert(self):
        insert_query = """
        PREFIX sp:  <http://sync.params/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        DROP GRAPH sp:param;
        INSERT DATA { 
            GRAPH sp:param {
                sp:param sp:step "0"^^xsd:integer
            }
        };
        """
        self.store.update_query(insert_query)

    def _perform_init_store(self):
        """
        Add the sync parameters to the store.
        Fetch the last page per feed type
        """

        feeds = [ResourceEnum.assets.name, ResourceEnum.agents.name, ResourceEnum.assetrelaties.name,
                 ResourceEnum.betrokkenerelaties.name]

        last_page_dict = {}

        # use multithreading
        executor = ThreadPoolExecutor()
        futures = [executor.submit(self._save_last_feedevent_to_params, page_size=100, feed=feed,
                                   last_page_dict=last_page_dict)
                   for feed in feeds]
        concurrent.futures.wait(futures)

        insert_query = """
        PREFIX sp:  <http://sync.params/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        INSERT DATA { 
            GRAPH sp:param { """

        for feed in feeds:
            insert_query += f"""
            sp:param sp:fill sp:{feed} .
            sp:{feed} sp:label "{feed}" .
            sp:{feed} sp:last_page "{last_page_dict[feed]['page']}"^^xsd:integer . 
            sp:{feed} sp:last_event_uuid "{last_page_dict[feed]['event_uuid']}" . """
        insert_query += "}};"

        self.store.update_query(insert_query)

        change_step_query = """
        PREFIX sp:  <http://sync.params/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        INSERT DATA { 
            GRAPH sp:param {
                sp:param sp:step "2"^^xsd:integer
            }
        };       
        DELETE DATA { 
            GRAPH sp:param {
                sp:param sp:step "1"^^xsd:integer
            }
        };
        """
        self.store.update_query(change_step_query)

    def _save_last_feedevent_to_params(self, page_size: int, feed: str, last_page_dict: dict) -> dict:
        logging.info(f'Getting last page of current feed for {feed}')

        while True:
            try:
                event_page = self.eminfra_importer.get_events_from_proxyfeed(page_size=page_size, resource=feed)
                self_link = next(l for l in event_page['links'] if l['rel'] == 'self')
                current_page_num = int(self_link['href'][1:].split('/')[0])
                break
            except Exception as ex:
                logging.error(ex.args[0])
                time.sleep(60)

        # find last event_id
        entries = event_page['entries']
        last_event_uuid = entries[0]['id']
        logging.info(f'retrieved last page of current feed for {feed} (page: {current_page_num})')

        last_page_dict[feed] = {
            'event_uuid': last_event_uuid,
            'page': current_page_num,
            'last_update_utc': None}

    def get_step(self):
        select_q = """PREFIX sp:    <http://sync.params/>
        SELECT ?o WHERE { GRAPH sp:param { sp:param sp:step ?o}}"""
        result_step = self.store.query(select_q)
        return list(result_step)[0][0].toPython()

    def _perform_filling(self):
        feeds = [ResourceEnum.agents]

        for feed_type in feeds:
            # create fill manager and start filling
            fill_manager = self._create_fill_manager(feed_type=feed_type)
            fill_manager.fill()

    def _create_fill_manager(self, feed_type: ResourceEnum) -> FillManager:
        return FillManager(store=self.store, eminfra_importer=self.eminfra_importer, feed_type=feed_type)


