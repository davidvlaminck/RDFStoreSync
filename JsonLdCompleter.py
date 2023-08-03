import logging
import sqlite3
from pathlib import Path

from ResourceEnum import ResourceEnum

CURRENT_DIR = Path(__file__).parent


class JsonLdCompleter:
    context_dir = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'asset': 'https://data.awvvlaanderen.be/id/asset/',
        'lgc': 'https://wegenenverkeer.data.vlaanderen.be/oef/legacy/',
        'ins': 'https://wegenenverkeer.data.vlaanderen.be/oef/inspectie/',
        'ond': 'https://wegenenverkeer.data.vlaanderen.be/oef/onderhoud/',
        'loc': 'https://wegenenverkeer.data.vlaanderen.be/oef/locatie/',
        'tz': 'https://wegenenverkeer.data.vlaanderen.be/oef/toezicht/',
        'geo': 'https://wegenenverkeer.data.vlaanderen.be/oef/geometrie/',
        'grp': 'https://wegenenverkeer.data.vlaanderen.be/oef/groepering/',
        'bz': 'https://wegenenverkeer.data.vlaanderen.be/oef/bezoekfiche/',
        'imel': 'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#',
        'app': 'http://example.org/ApplicationSchema#',
        'abs': 'https://wegenenverkeer.data.vlaanderen.be/ns/abstracten#',
        'geosparql': 'http://www.opengis.net/ont/geosparql#',
        'onderdeel': 'https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#',
        'installatie': 'https://wegenenverkeer.data.vlaanderen.be/ns/installatie#',
        'purl': 'http://purl.org/dc/terms/',
        'schema': 'https://schema.org/'
    }

    def __init__(self, type: ResourceEnum, otl_db_path: Path = Path(CURRENT_DIR / 'OTL.db')):
        self.valid_uris = self.get_otl_uris_from_db(otl_db_path)
        self.type = type

    @staticmethod
    def transform_if_http_value(value):
        if isinstance(value, str) and value.startswith('http'):
            return {"@id": value}
        return value

    def fix_dict(self, old_dict):
        new_dict = {}
        for k, v in old_dict.items():
            if isinstance(v, dict):
                v = self.fix_dict(v)
            elif isinstance(v, list):
                new_list = []
                for item in v:
                    if isinstance(item, dict):
                        new_list.append(self.fix_dict(item))
                    else:
                        new_list.append(self.transform_if_http_value(item))
                v = new_list

            if ':' in k:
                new_dict[k] = self.transform_if_http_value(v)
            elif k in ['RelatieObject.bron', 'RelatieObject.doel']:
                new_dict[
                    'https://wegenenverkeer.data.vlaanderen.be/ns/implementatieelement#' + k] = self.transform_if_http_value(
                    v)
            elif k not in ['@type', '@id']:
                new_dict[self.valid_uris[k]] = self.transform_if_http_value(v)
            else:
                new_dict[k] = v
        return new_dict

    @staticmethod
    def get_otl_uris_from_db(otl_db_path) -> dict:
        con = sqlite3.connect(otl_db_path)
        cur = con.cursor()
        d = {}
        res = cur.execute("""
SELECT uri FROM OSLOAttributen 
UNION SELECT uri FROM OSLODatatypeComplexAttributen 
UNION SELECT uri FROM OSLODatatypePrimitiveAttributen 
UNION SELECT uri FROM OSLODatatypeUnionAttributen""")
        for row in res.fetchall():
            uri = row[0]
            if '#' in uri:
                short_uri = uri[uri.find('#') + 1:]
            else:
                short_uri = uri[uri.rfind('/') + 1:]
            d[short_uri] = uri

        con.close()
        return d

    def enhance_asset_graph(self, asset_graph):
        asset_graph['@context'] = self.context_dir
        new_asset_list = []
        for asset in asset_graph['@graph']:
            fixed_dict = self.fix_dict(asset)
            if self.type == ResourceEnum.assets:
                fixed_dict = JsonLdCompleter.add_exact_geometry(fixed_dict)
            new_asset_list.append({
                "@id": fixed_dict['@id'],
                '@graph': [fixed_dict]
            })

        asset_graph['@graph'] = new_asset_list

    @staticmethod
    def add_exact_geometry(asset_dict):
        if 'geo:Geometrie.log' in asset_dict:
            if len(asset_dict['geo:Geometrie.log']) > 0:
                first_log = asset_dict['geo:Geometrie.log'][0]
                geom = first_log['geo:DtcLog.geometrie']
                if len(geom.items()) > 0:
                    wkt_str = next(iter(geom.values()))
                    asset_dict['http://example.org/ApplicationSchema#hasExactGeometry'] = \
                        JsonLdCompleter.get_exact_geometry_from_wkt_str(wkt_str, id=asset_dict['@id'])
                return asset_dict

        if 'loc:Locatie.geometrie' in asset_dict:
            wkt_str = asset_dict['loc:Locatie.geometrie']
            if wkt_str != '':
                asset_dict['http://example.org/ApplicationSchema#hasExactGeometry'] = \
                    JsonLdCompleter.get_exact_geometry_from_wkt_str(wkt_str, id=asset_dict['@id'])
                return asset_dict

        if 'loc:Locatie.puntlocatie' in asset_dict and 'loc:3Dpunt.puntgeometrie' in asset_dict[
            'loc:Locatie.puntlocatie']:
            if 'loc:DtcCoord.lambert72' not in asset_dict['loc:Locatie.puntlocatie']['loc:3Dpunt.puntgeometrie']:
                return asset_dict
            coords = asset_dict['loc:Locatie.puntlocatie']['loc:3Dpunt.puntgeometrie']['loc:DtcCoord.lambert72']
            x = coords['loc:DtcCoordLambert72.xcoordinaat']
            y = coords['loc:DtcCoordLambert72.ycoordinaat']
            z = coords['loc:DtcCoordLambert72.zcoordinaat']

            if x == '' or x is None or x == 0 or x == 0:
                raise ValueError(asset_dict)

            wkt_str = 'POINT Z ({} {} {})'.format(x, y, z)

            asset_dict['http://example.org/ApplicationSchema#hasExactGeometry'] = \
                JsonLdCompleter.get_exact_geometry_from_wkt_str(wkt_str, id=asset_dict['@id'])

        return asset_dict

    @staticmethod
    def get_exact_geometry_from_wkt_str(wkt_str, id):
        geom_type = None
        if wkt_str.startswith('POINT'):
            geom_type = 'http://www.opengis.net/ont/sf#Point'
        elif wkt_str.startswith('MULTIPOINT'):
            geom_type = 'http://www.opengis.net/ont/sf#MultiPoint'
        elif wkt_str.startswith('POLYGON'):
            geom_type = 'http://www.opengis.net/ont/sf#Polygon'
        elif wkt_str.startswith('LINESTRING'):
            geom_type = 'http://www.opengis.net/ont/sf#LineString'
        elif wkt_str.startswith('MULTILINESTRING'):
            geom_type = 'http://www.opengis.net/ont/sf#MultiLineString'
        elif wkt_str.startswith('MULTIPOLYGON'):
            geom_type = 'http://www.opengis.net/ont/sf#MultiPolygon'
        elif wkt_str.startswith('GEOMETRYCOLLECTION'):
            geom_type = 'http://www.opengis.net/ont/sf#GeometryCollection'
        else:
            logging.error(f'Unknown geometry type: {wkt_str}')
        return {
            "@id": id + '_geometry',
            "@type": geom_type,
            "http://www.opengis.net/ont/geosparql#asWKT": {
                "@value": f"<http://www.opengis.net/def/crs/EPSG/9.9.1/31370> {wkt_str}",
                "@type": "http://www.opengis.net/ont/geosparql#wktLiteral"
            }}
