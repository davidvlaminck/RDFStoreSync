from rdflib import Dataset, URIRef, Literal, XSD

from RdflibEndpoint import RdflibEndpoint
from StoreSyncer import StoreSyncer


def create_empty_store():
    d = Dataset()
    rdflib_endpoint = RdflibEndpoint(d)
    store_syncer = StoreSyncer(store=rdflib_endpoint)
    return store_syncer


def test_fresh_init():
    store_syncer = create_empty_store()
    count_triples = store_syncer.store.count_triples(graph_uri='http://sync.params/param')
    assert count_triples == 0
    assert not store_syncer.check_for_sync_params()


def test_init_store():
    store_syncer = create_empty_store()
    store_syncer.init_store()
    count_triples = store_syncer.store.count_triples(graph_uri='http://sync.params/param')
    assert not count_triples == 0
    select_q = """PREFIX sp:    <http://sync.params/>
    SELECT ?o WHERE { GRAPH sp:param { sp:param sp:step ?o}}"""
    result_step = store_syncer.store.query(select_q)
    assert int(list(result_step)[0][0].toPython()) == 1


def test_reset_store_insert():
    store_syncer = create_empty_store()
    store_syncer.init_store()
    store_syncer.reset_store_insert()
    select_q = """PREFIX sp:    <http://sync.params/>
    SELECT ?o WHERE { GRAPH sp:param { sp:param sp:step ?o}}"""
    result_step = store_syncer.store.query(select_q)
    assert int(list(result_step)[0][0].toPython()) == 0


def test_reset_store():
    store_syncer = create_empty_store()
    store_syncer.reset_store_insert()
    store_syncer.store.endpoint.add((URIRef('http://ex.com/1'), URIRef('http://ex.com/2'),
           URIRef('http://ex.com/3'), URIRef('http://ex.com/4')))

    store_syncer.reset_store()

    select_q = """PREFIX sp:    <http://sync.params/>
    SELECT ?o WHERE { GRAPH sp:param { sp:param sp:step ?o}}"""
    result_step = store_syncer.store.query(select_q)
    assert int(list(result_step)[0][0].toPython()) == 1


def test_perform_init_store():
    store_syncer = create_empty_store()
    store_syncer.init_store()
    count_triples = store_syncer.perform_init_store()
    assert not count_triples == 0
    select_q = """PREFIX sp:    <http://sync.params/>
    SELECT ?o WHERE { GRAPH sp:param { sp:param sp:step ?o}}"""
    result_step = store_syncer.store.query(select_q)
    assert int(list(result_step)[0][0].toPython()) == 1