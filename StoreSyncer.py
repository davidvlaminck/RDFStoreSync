class StoreSyncer:
    def __init__(self, store):
        self.store = store

    def check_for_sync_params(self) -> bool:
        """
        Check if the store has the sync parameters.
        """
        result = self.store.query("""ASK { GRAPH <http://sync.params/param> { ?s ?p ?o }}""")
        return bool(list(result)[0])

    def init_store(self):
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

    def print_quads(self, limit:int = 100):
        print(self.store.endpoint.serialize(format='nquads', limit=limit))

    def reset_store(self):
        drop_query = """CLEAR ALL;"""
        self.store.update_query(drop_query)
        self.init_store()

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

    def perform_init_store(self):
        """
        Add the sync parameters to the store.
        Fetch the last page per feed type
        """
        
        insert_query = """
PREFIX sp:  <http://sync.params/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
INSERT DATA { 
    GRAPH sp:param {
        sp:param sp:fill [ 
            sp:label "assets" ;
            sp:last_page "1"^^xsd:integer  ;
            sp:last_cursor "cursor" ;
            sp:filling [
                sp:ongoing true ;
                sp:cursor "" ;
                sp:update_datum "<timestamp>"^^xsd:dateTime" ]
        ] .
    }
};
"""
        self.store.update_query(insert_query)

# dt : "2005-01-01T00:00:00Z"^^xsd:dateTime