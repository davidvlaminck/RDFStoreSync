from pathlib import Path
from unittest.mock import Mock

from rdflib import Dataset, URIRef

from EMInfraImporter import EMInfraImporter
from JWTRequester import JWTRequester
from RdflibEndpoint import RdflibEndpoint
from RequestHandler import RequestHandler
from StoreSyncer import StoreSyncer
from UnitTests.EMInfraAPIMock import EMInfraAPIMock


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


def create_empty_store():
    d = Dataset()
    rdflib_endpoint = RdflibEndpoint(d)
    requester_mock = JWTRequester(private_key_path=Path(), client_id='')
    handler_mock = RequestHandler(requester_mock)
    em_infra_importer = EMInfraImporter(handler_mock)
    handler_mock.get_jsondict = Mock(side_effect=return_fake_jsondict)
    store_syncer = StoreSyncer(store=rdflib_endpoint, eminfra_importer=em_infra_importer)
    return store_syncer


def test_fresh_init():
    store_syncer = create_empty_store()
    count_triples = store_syncer.store.count_triples(graph_uri='http://sync.params/param')
    assert count_triples == 0
    assert not store_syncer.check_for_sync_params()


def test_init_store():
    store_syncer = create_empty_store()
    store_syncer._init_store()
    count_triples = store_syncer.store.count_triples(graph_uri='http://sync.params/param')
    assert not count_triples == 0
    assert store_syncer.get_step() == 1


def test_reset_store_insert():
    store_syncer = create_empty_store()
    store_syncer._init_store()
    store_syncer.reset_store_insert()
    assert store_syncer.get_step() == 0


def test_reset_store():
    store_syncer = create_empty_store()
    store_syncer.reset_store_insert()
    store_syncer.store.endpoint.add((URIRef('http://ex.com/1'), URIRef('http://ex.com/2'),
           URIRef('http://ex.com/3'), URIRef('http://ex.com/4')))

    store_syncer._reset_store()

    assert store_syncer.get_step() == 1


def test_perform_init_store():
    store_syncer = create_empty_store()
    store_syncer._init_store()
    assert store_syncer.get_step() == 1

    store_syncer._perform_init_store()
    store_syncer.print_quads()

    select_q = """PREFIX sp:    <http://sync.params/>
    SELECT ?page ?event_uuid WHERE { GRAPH sp:param { 
        sp:param sp:fill ?bn .
        ?bn sp:label "assets" .
        ?bn sp:last_page ?page .
        ?bn sp:last_event_uuid ?event_uuid
    }}"""
    result = store_syncer.store.query(select_q)
    result_dict = list(result)[0].asdict()

    assert result_dict['page'].toPython() == 200965
    assert result_dict['event_uuid'].toPython() == 'aa534f87-c6ff-4502-a73b-bb075a1b832e'

    select_q = """PREFIX sp:    <http://sync.params/>
    SELECT ?page ?event_uuid WHERE { GRAPH sp:param { 
        sp:param sp:fill ?bn .
        ?bn sp:label "assetrelaties" .
        ?bn sp:last_page ?page .
        ?bn sp:last_event_uuid ?event_uuid
    }}"""
    result = store_syncer.store.query(select_q)
    result_dict = list(result)[0].asdict()

    assert result_dict['page'].toPython() == 20
    assert result_dict['event_uuid'].toPython() == '00000001-0000-0000-0000-000000000001'

    assert store_syncer.get_step() == 2

    store_syncer.store.endpoint.serialize(destination="params.jsonld", format='json-ld')


def test_get_step():
    store_syncer = create_empty_store()
    store_syncer._init_store()
    step = store_syncer.get_step()
    assert step == 1
