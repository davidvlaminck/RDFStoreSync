import json
from pathlib import Path
from unittest.mock import Mock

from rdflib import Dataset, URIRef
from requests import Response

from EMInfraImporter import EMInfraImporter
from FillManager import FillManager
from JWTRequester import JWTRequester
from RdflibEndpoint import RdflibEndpoint
from RequestHandler import RequestHandler
from ResourceEnum import ResourceEnum
from StoreSyncer import StoreSyncer
from UnitTests.EMInfraAPIMock import EMInfraAPIMock


def return_fake_perform_post_request(*args, **kwargs):
    if kwargs['url'] == 'core/api/otl/agents/search' and kwargs['json_data']['fromCursor'] is None:
        response = Mock(Response)
        response.content = json.dumps(EMInfraAPIMock.agents_first_page).encode('utf-8')
        response.headers = {'em-paging-next-cursor': 'cursor1'}
        return response
    elif kwargs['url'] == 'core/api/otl/agents/search' and kwargs['json_data']['fromCursor'] == 'cursor1':
        response = Mock(Response)
        response.content = json.dumps(EMInfraAPIMock.agents_second_page).encode('utf-8')
        response.headers = {'em-paging-next-cursor': 'cursor2'}
        return response
    elif kwargs['url'] == 'core/api/otl/agents/search' and kwargs['json_data']['fromCursor'] == 'cursor2':
        response = Mock(Response)
        response.content = json.dumps(EMInfraAPIMock.agents_third_page).encode('utf-8')
        response.headers = {'em-paging-next-cursor': ''}
        return response
    raise Exception(f'Could not return fake post request for url: {kwargs["url"]} and params: {kwargs["json_data"]}')


def return_fake_jsondict(*args, **kwargs):
    if kwargs['url'] == 'feedproxy/feed/assets':
        return EMInfraAPIMock.last_feed_page_assets
    elif kwargs['url'] == 'feedproxy/feed/agents':
        return EMInfraAPIMock.last_feed_page_agents
    elif kwargs['url'] == 'feedproxy/feed/assetrelaties':
        return EMInfraAPIMock.last_feed_page_assetrelaties
    elif kwargs['url'] == 'feedproxy/feed/betrokkenerelaties':
        return EMInfraAPIMock.last_feed_page_betrokkenerelaties
    raise Exception("Could not return fake jsondict for url: " + kwargs['url'])


def create_fill_manager(feed_type) -> FillManager:
    d = Dataset()
    rdflib_endpoint = RdflibEndpoint(d)
    requester_mock = JWTRequester(private_key_path=Path(), client_id='')
    handler_mock = RequestHandler(requester_mock)
    em_infra_importer = EMInfraImporter(handler_mock)
    handler_mock.perform_post_request = Mock(side_effect=return_fake_perform_post_request)
    handler_mock.get_jsondict = Mock(side_effect=return_fake_jsondict)
    store_syncer = StoreSyncer(store=rdflib_endpoint, eminfra_importer=em_infra_importer)
    store_syncer._init_store()
    store_syncer._perform_init_store()
    return store_syncer._create_fill_manager(feed_type)


def test_get_filling_param_empty():
    fill_manager = create_fill_manager(ResourceEnum.agents)
    fill_param = fill_manager._get_filling_params()
    assert fill_param == {}


def test_get_filling_param_one_fill():
    fill_manager = create_fill_manager(ResourceEnum.agents)
    fill_param = fill_manager._get_filling_params()
    assert fill_param == {}

    def new_return_fake_perform_post_request(*args, **kwargs):
        if kwargs['url'] == 'core/api/otl/agents/search' and kwargs['json_data']['fromCursor'] is None:
            response = Mock(Response)
            response.content = json.dumps(EMInfraAPIMock.agents_first_page).encode('utf-8')
            response.headers = {'em-paging-next-cursor': 'cursor1'}
            return response
        elif kwargs['url'] == 'core/api/otl/agents/search' and kwargs['json_data']['fromCursor'] == 'cursor1':
            response = Mock(Response)
            response.content = json.dumps(EMInfraAPIMock.agents_second_page).encode('utf-8')
            response.headers = {'em-paging-next-cursor': ''}
            return response

    fill_manager.eminfra_importer.request_handler.perform_post_request = new_return_fake_perform_post_request
    fill_manager.fill()

    fill_param = fill_manager._get_filling_params()
    assert fill_param != {}
    assert str(fill_param['cursor']) == ''
    assert not bool(fill_param['state'])


def test_fill_multiple_fills():
    fill_manager = create_fill_manager(ResourceEnum.agents)
    fill_manager.fill(False)

    fill_param = fill_manager._get_filling_params()
    assert fill_param != {}
    assert str(fill_param['cursor']) == ''
    assert not bool(fill_param['state'])

    select_q = """SELECT ?agent WHERE { GRAPH ?g { ?agent a <http://purl.org/dc/terms/Agent> } }"""
    result = fill_manager.store.query(select_q)
    assert len(result) == 4

    select_q = f"""PREFIX sp:    <http://sync.params/>
    SELECT ?state ?cursor ?update_timestamp 
    WHERE {{ 
        {{
            SELECT (MAX(?update_timestamp) as ?max_update_timestamp)
            WHERE {{ 
                GRAPH sp:param {{ 
                    sp:param sp:fill ?bn .
                    ?bn sp:label "{fill_manager.feed_type.value}" .
                    ?bn sp:filling ?fill_node .
                    ?fill_node sp:update_timestamp ?update_timestamp 
                }} 
            }} 
        }}
        GRAPH sp:param {{ 
        sp:param sp:fill ?bn .
        ?bn sp:label "{fill_manager.feed_type.value}" .
        ?bn sp:filling ?fill_node .
        ?fill_node sp:state ?state .
        ?fill_node sp:cursor ?cursor .
        ?fill_node sp:update_timestamp ?update_timestamp
        }} 
    FILTER (?update_timestamp != ?max_update_timestamp)
    }}
    """
    result = fill_manager.store.query(select_q)
    assert len(result) == 0
    fill_manager.print_quads()
