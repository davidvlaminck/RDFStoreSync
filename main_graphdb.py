import logging

from rdflib import Dataset

from EMInfraImporter import EMInfraImporter
from GraphDbEndpoint import GraphDbEndpoint
from RdflibEndpoint import RdflibEndpoint
from RequestHandler import RequestHandler
from RequesterFactory import RequesterFactory
from SettingsManager import SettingsManager
from StoreSyncer import StoreSyncer

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    environment = 'prd'

    settings_manager = SettingsManager(
        settings_path='/home/davidlinux/Documents/AWV/resources/settings_AwvinfraPostGISSyncer.json')
    db_settings = settings_manager.settings['databases'][environment]

    # connector = PostGISConnector(**db_settings)

    requester = RequesterFactory.create_requester(settings=settings_manager.settings, auth_type='JWT', env=environment,
                                                  multiprocessing_safe=True)
    request_handler = RequestHandler(requester)
    eminfra_importer = EMInfraImporter(request_handler)

    graphdb_endpoint = GraphDbEndpoint(url='http://localhost:7200/repositories/awvinfra')
    store_syncer = StoreSyncer(store=graphdb_endpoint, eminfra_importer=eminfra_importer)
    store_syncer.reset_store_insert()
    store_syncer.run()
