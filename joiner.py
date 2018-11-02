from jinja2 import Template
import configparser
import xml.etree.ElementTree as ET
import requests
import json
import re
from abc import ABC
import abc
import threading
import psycopg2
import sys

class Model(ABC):
    """ Abstract class for defining the endpoints to search """

    @abc.abstractmethod
    def obtain_id(self, x, y, sf_function):
        """
        Obtain polygon layer identifier using specified function

        :param x: longitude of the point
        :param y: latitude of the point
        :type x: double
        :type y: double
        :returns: identifier
        :rtype: string
        :raises PIPError: raised when PIP fails and no XML result is received
        """
        raise NotImplementedError

class RegisterModel(ABC):
    """ Abstract class for defining the endpoints to use as an index """

    @abc.abstractmethod
    def get_point(self, id):
        """
        Get longitude and latitude for record by ID

        :param id: id for document to decode
        :type id: string
        :returns: latitude, longiture
        :rtype: double, double
        :raises FetchPointError: raised when call to retrieve point fails
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_ids(self, batch=1, batch_size=10):
        """
        Get IDs using register
        
        :param batch: page to start getting items from
        :param batch_size: number of items to retrieve
        :type batch: integer
        :type batch_size: integer
        :returns: IDs for the specified number of items and whether there are more
        :rtype: list, boolean
        :raises FetchIdBatchError: raised when call to retrieve IDs fails
        """
        raise NotImplementedError

class PIPError(Exception):
    """ Error when conducting Point in Polygon """
    pass

class FetchIdBatchError(Exception):
    """ Error when retrieving a set of Point IDs """

class FetchPointError(Exception):
    """ Error when retrieving Point from ID """

class InitialisationError(Exception):
    """ Error when initialising the DBModel """

class WFSModel(Model):
    """ WFS endpoint to search """

    # Template for WFS requests
    _URL_TEMPLATE = Template(
        "{{ url }}?service=WFS&request=GetFeature&version=1.0.0&typeName={{ layer }}"
        "&outputFormat=GML2&FILTER=%3CFilter%20xmlns=%22http://www.opengis.net/ogc%22"
        "%20xmlns:gml=%22http://www.opengis.net/gml%22%3E%3C{{ sf_function }}%3E"
        "%3CPropertyName%3E{{ geometry_field }}%3C/PropertyName%3E"
        "%3Cgml:Point%20srsName=%22EPSG:4283%22%3E%3Cgml:coordinates%3E{{ x }},"
        "{{ y }}%3C/gml:coordinates%3E%3C/gml:Point%3E%3C/{{ sf_function }}%3E"
        "%3C/Filter%3E&PropertyName={{ layer_id }}"
    )

    _ns = None
    _url = None
    _layer = None
    _geometry_field = None
    _layer_id = None

    def __init__(self, ns_short, ns_url, url, layer, geometry_field, layer_id):
        """
        Creator

        :param ns_short: Prefix of the namespace for the layer identifier
        :type ns_short: string
        :param ns_url: URL of the namespace for the layer identifier
        :type ns_url: string
        :param url: URL of the WFS Service endpoint
        :type url: string
        :param layer: Name of the layer to query
        :type layer: string
        :param geometry_field: Name of the geometry attribute to use for PIP
        :type geometry_field: string
        :param layer_id: Name of the identifier field (including prefix)
        :type layer_id: string
        """
        self._ns = {
            'gml': 'http://www.opengis.net/gml',
            'wfs': 'http://www.opengis.net/wfs',
            ns_short: ns_url
        }
        self._url = url
        self._layer = layer
        self._geometry_field = geometry_field
        self._layer_id = layer_id


    def obtain_id(self, x, y, sf_function):
        id = None
        query_url = self._URL_TEMPLATE.render(
            url=self._url, layer=self._layer, geometry_field=self._geometry_field,
            layer_id=self._layer_id, x=x, y=y, sf_function=sf_function
        )
        try:
            response = requests.get(query_url)
            root = ET.fromstring(response.text)
            for item in root.findall('gml:featureMember', self._ns):
                feature = item.find(self._layer, self._ns)
                id = feature.find(self._layer_id, self._ns).text
        except ET.ParseError as pe:
            raise PIPError("PIP FAILED")
        return id

class DBModel(RegisterModel):
    """ Database endpoint to use as an index """

    _connection_string = None
    _count = 0

    def __init__(self, endpoint, initialiser="SELECT count(*) FROM gnaf.address_detail"):
        """
        Creator

        :param endpoint: psycopg2 PostgreSQL database connection string
        :type endpoint: string
        :param initialiser: Query used to count the number of records to process
        :type initialiser: string
        """
        self._connection_string = endpoint
        try:
            row = self.run_query(query=initialiser, parameters=(id,), get_all=False)
            self._count = int(row[0])
        except Exception as ce:
            raise InitialisationError(id)

    def get_point(self, id, select_statement = (
            "SELECT longitude, latitude "
            "FROM gnaf.address_default_geocode "
            "WHERE address_detail_pid = %s"
    )):
        x = None
        y = None
        try:
            row = self.run_query(query=select_statement, parameters=(id,), get_all=False)
            x = row[0]
            y = row[1]
        except Exception as ce:
            raise FetchPointError(id)
        return x, y
        
    def get_ids(self, batch=1, batch_size=10, select_statement = (
            "SELECT address_detail_pid "
            "FROM gnaf.address_detail "
            "ORDER BY address_detail_pid "
            "LIMIT %s "
            "OFFSET %s "
    )):
        rows = []
        try:
            rows = self.run_query(select_statement, (batch_size, batch_size*(batch-1)))
        except Exception as ce:
            raise FetchIdBatchError(select_statement)
        return [row[0] for row in rows], (batch_size*batch) < self._count

    def run_query(self, query, parameters=None, get_all=True):
        """
        Run a query

        :param query: Query to execute
        :type query: string
        :param parameters: Parameters to issue to query
        :type parameters: tuple
        :param get_all: IF true THEN fetchall ELSE fetchone
        :type get_all: boolean
        :return: results of the cursor fetch
        :rtype: list
        """
        rows = None
        conn = psycopg2.connect(self._connection_string)
        cur = conn.cursor()
        cur.execute(query, parameters)
        if get_all:
            rows = cur.fetchall()
        else:
            rows = cur.fetchone()
        return rows

class LDAPIModel(RegisterModel):
    """ WFS endpoint to use as an index """

    _endpoint_url = None

    def __init__(self, endpoint, initialiser=None):
        """
        Creator

        :param endpoint: The pyLDAPI Python Linked Data API endpoint
        :type endpoint: string
        :param initialiser: Task or string used to prepare the endpoint - currently unused
        :type initialiser: string
        """
        self._endpoint_url = endpoint

    def get_point(self, id):
        x = None
        y = None
        try:
            response = requests.get(id, headers={'Accept': 'application/json'})
            m = re.search('POINT\(-?\d+\.\d+ -?\d+\.\d+\)', response.text)
            if m:
                m2 = re.search('-?\d+\.\d+ -?\d+\.\d+', m.group(0))
                if m2:
                    coordinates = m2.group(0).split(' ')
                    x = coordinates[0]
                    y = coordinates[1]
        except requests.exceptions.ConnectionError as ce:
            raise FetchPointError(id)
        return x, y
        
    def get_ids(self, batch=1, batch_size=10):
        query_url = '{}?page={}&per_page={}'.format(self._endpoint_url, batch, batch_size)
        try:
            response = requests.get(query_url, headers={'Accept': 'application/json'})
            json_data = json.loads(response.text)
        except requests.exceptions.ConnectionError as ce:
            raise FetchIdBatchError(query_url)
        return [item[0] for item in json_data['register_items']], 'next' in response.links

def write_output(file, lines):
    """
    Write contents of a list as lines to a specified file
    :param file: The file to write output to
    :type file: string
    :param lines: The lines to write to the specified file\
    :type lines: list
    :return: None
    """
    with open(file, 'a') as file:
        for line in lines:
            file.write(line)
        file.close()
        
def pip(row_id, point_id, sf_function):
    """
    Perform point in polygon search using specified Simple Features function

    :param row_id: An identifier for the link match
    :type row_id: string
    :param point_id: An identifier for the point in the index
    :type point_id: string
    :param sf_function: The name of the Simple Features function e.g., Contains, Intersects
    :return: None
    """
    try:
        x, y = point_model.get_point(point_id)
        try:
            id = polygon_model.obtain_id(x=x, y=y, sf_function=sf_function)
            row = '{},{},{}\n'.format(row_id, point_id, id)
        except PIPError as pe:
            row = '{},{},{}\n'.format(row_id, point_id, "PIPFAIL")
            print(row)
    except FetchPointError as fpe:
        row = '{},{},{}\n'.format(row_id, point_id, "POINTFAIL")
        print(row)
    while global_lock.locked():
        continue
    global_lock.acquire()
    cache.append(row)
    global_lock.release()

if __name__ == "__main__":

    config = configparser.ConfigParser()
    r = config.read('joiner.config')
    url = config['DEFAULT']['endpoint']
    layer = config['DEFAULT']['layer']
    geometry_field = config['DEFAULT']['geom']
    layer_id = config['DEFAULT']['layerid']
    ns_short = config['DEFAULT']['nsshort']
    ns_url = config['DEFAULT']['nsurl']
    register_endpoint = config['DEFAULT']['register_endpoint']
    register_model = config['DEFAULT']['register_model']
    function = config['DEFAULT']['function']
    batch_start = int(config['DEFAULT']['start'])
    batch_size = int(config['DEFAULT']['batch_size'])
    batch_stop = int(config['DEFAULT']['stop'])
    output_file = config['DEFAULT']['output_file']
    threads = int(config['DEFAULT']['threads'])
    i = int(config['DEFAULT']['batch_id'])

    model = getattr(sys.modules[__name__], register_model)
    point_model = model(endpoint=register_endpoint)
    polygon_model = WFSModel(
        ns_short=ns_short, ns_url=ns_url, url=url, layer=layer,
        geometry_field=geometry_field, layer_id=layer_id
    )

    loop = True

    global_lock = threading.Lock()
    cache = None

    while batch_start < batch_stop and loop:
        try:
            identifiers, loop = point_model.get_ids(batch=batch_start, batch_size=batch_size)
            records = len(identifiers)
            indexer = 0
            while indexer < records:
                cache = []
                processing_threads = []
                iterations = threads
                remainder = records - indexer
                if remainder < iterations:
                    iterations = remainder
                for counter in range(1, iterations+1):
                    t = threading.Thread(target=pip, args=[i, identifiers[indexer], function])
                    processing_threads.append(t)
                    t.start()
                    i += 1
                    indexer += 1
                [thread.join() for thread in processing_threads]
                write_output(output_file, cache)
        except FetchIdBatchError as id_error:
            print(id_error.message)
        batch_start += 1
