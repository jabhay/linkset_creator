from joiner import WFSModel, DBModel, LDAPIModel
import joiner
import pytest
from mock import patch
import requests
import xml.etree.ElementTree as ET
import configparser

class TestWFSModel:

    _NS_SHORT='ahgf_shcatch'
    _NS_URL='http://linked.data.gov.au/dataset/geof/v2/ahgf_shcatch'
    _URL='http://geofabricld.net/geoserver/ows'
    _LAYER='ahgf_shcatch:AHGFCatchment'
    _GEOMETRY_FIELD='shape'
    _LAYER_ID='ahgf_shcatch:hydroid'
    _REQUEST_URL=(
        "http://geofabricld.net/geoserver/ows?service=WFS&request=GetFeature&version=1.0.0"
        "&typeName=ahgf_shcatch:AHGFCatchment&outputFormat=GML2&FILTER="
        "%3CFilter%20xmlns=%22http://www.opengis.net/ogc%22%20xmlns:gml=%22"
        "http://www.opengis.net/gml%22%3E%3CContains%3E%3CPropertyName%3Eshape"
        "%3C/PropertyName%3E%3Cgml:Point%20srsName=%22EPSG:4283%22%3E%3Cgml:coordinates%3E"
        "149.03865604,-35.20113263%3C/gml:coordinates%3E%3C/gml:Point%3E%3C/Contains%3E"
        "%3C/Filter%3E&PropertyName=ahgf_shcatch:hydroid"
    )
    _RESULT_XML=(
        '<?xml version="1.0" encoding="UTF-8"?><wfs:FeatureCollection xmlns="http://www.opengis.net/wfs" xmlns:wfs="http://www.opengis.net/wfs" xmlns:ahgf_shcatch="http://linked.data.gov.au/dataset/geof/v2/ahgf_shcatch" xmlns:gml="http://www.opengis.net/gml" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wfs http://52.62.131.116:80/geoserver/schemas/wfs/1.0.0/WFS-basic.xsd http://linked.data.gov.au/dataset/geof/v2/ahgf_shcatch http://52.62.131.116:80/geoserver/wfs?service=WFS&amp;version=1.0.0&amp;request=DescribeFeatureType&amp;typeName=ahgf_shcatch%3AAHGFCatchment"><gml:boundedBy><gml:null>unknown</gml:null></gml:boundedBy><gml:featureMember><ahgf_shcatch:AHGFCatchment fid="AHGFCatchment.488811"><ahgf_shcatch:hydroid>7155143</ahgf_shcatch:hydroid><ahgf_shcatch:ahgfftype>21</ahgf_shcatch:ahgfftype><ahgf_shcatch:ncb_id>1057987</ahgf_shcatch:ncb_id><ahgf_shcatch:srcfcname>NCBs</ahgf_shcatch:srcfcname><ahgf_shcatch:srcftype> </ahgf_shcatch:srcftype><ahgf_shcatch:fsource>GEOSCIENCE AUSTRALIA</ahgf_shcatch:fsource><ahgf_shcatch:attrsource>GEOSCIENCE AUSTRALIA</ahgf_shcatch:attrsource></ahgf_shcatch:AHGFCatchment></gml:featureMember></wfs:FeatureCollection>'
    )
    _ID='7155143'
    _LATITUDE=-35.20113263
    _LONGITUDE=149.03865604
    _FUNCTION='Contains'

    @pytest.fixture(scope="class")
    def wfs_polygon_model(self):
        wfs = WFSModel(
            ns_short=self._NS_SHORT, ns_url=self._NS_URL, url=self._URL, layer=self._LAYER,
            geometry_field=self._GEOMETRY_FIELD, layer_id=self._LAYER_ID
        )
        yield wfs

    @patch('requests.get')
    def test_obtain_id(self, mock_requests_get, wfs_polygon_model):
        mock_requests_get.return_value.text = self._RESULT_XML
        id = wfs_polygon_model.obtain_id(
            x=self._LONGITUDE, y=self._LATITUDE, sf_function=self._FUNCTION
        )
        assert id == self._ID
        mock_requests_get.side_effect = ET.ParseError
        with pytest.raises(joiner.PIPError):
            id = wfs_polygon_model.obtain_id(
                x=self._LONGITUDE, y=self._LATITUDE, sf_function=self._FUNCTION
            )


class TestDBModel:

    _RETURN_ALL_QUERY="SELECT %s AS a, %s AS b UNION SELECT %s, %s"
    _RETURN_ALL_QUERY_PARAM=(1,2,3,4)
    _RETURN_ONE_QUERY="SELECT %s"
    _RETURN_ONE_QUERY_PARAM=(1,)
    _COUNT=10
    _LATITUDE=-39.12345
    _LONGITUDE=147.12345
    _ID='123'

    @pytest.fixture(scope="class")
    def db_model(self):
        config = configparser.ConfigParser()
        r = config.read('joiner.config')
        endpoint_register = config['DEFAULT']['register_endpoint']
        db = DBModel(endpoint=endpoint_register,
            initialiser="SELECT {}".format(self._COUNT)
        )
        yield db

    @pytest.mark.parametrize("query, param, select_all, result", [
        (_RETURN_ALL_QUERY, _RETURN_ALL_QUERY_PARAM, True, [(1,2),(3,4)]),
        (_RETURN_ONE_QUERY, _RETURN_ONE_QUERY_PARAM, True, [(1,)])
    ])
    def test_run_query(self, query, param, select_all, result, db_model):
        rows = db_model.run_query(query=query, parameters=param, get_all=select_all)
        assert result == rows

    @pytest.mark.parametrize("batch, batch_size, offset, results, more", [
        (1, 10, 0, [(1,),(2,),(3,),(4,),(5,),(6,),(7,),(8,),(9,),(10,)], False),
        (2, 4, 4, [(5,),(6,),(7,),(8,)], True)
    ])
    @patch('joiner.DBModel.run_query')
    def test_get_ids(
            self, mock_run_query, batch, batch_size, offset, results, more, db_model
    ):
        mock_run_query.return_value = results
        rows, more_records = db_model.get_ids(
            batch=batch, batch_size=batch_size, select_statement=self._RETURN_ONE_QUERY
        )
        mock_run_query.assert_called_once_with(self._RETURN_ONE_QUERY, (batch_size, offset))
        assert more == more_records
        assert len(results) == len(rows)
        mock_run_query.side_effect = Exception
        with pytest.raises(joiner.FetchIdBatchError):
            rows, more_records = db_model.get_ids(
                batch=batch, batch_size=batch_size, select_statement=self._RETURN_ONE_QUERY
            )

    @patch('joiner.DBModel.run_query')
    def test_get_point(
            self, mock_run_query, db_model
    ):
        mock_run_query.return_value = (self._LONGITUDE, self._LATITUDE)
        x, y = db_model.get_point(id=self._ID, select_statement=self._RETURN_ONE_QUERY)
        mock_run_query.assert_called_once_with(
            query=self._RETURN_ONE_QUERY, parameters=(self._ID,), get_all=False
        )
        assert x == self._LONGITUDE
        assert y == self._LATITUDE
        mock_run_query.side_effect = Exception
        with pytest.raises(joiner.FetchPointError):
            x, y = db_model.get_point(id=self._ID, select_statement=self._RETURN_ONE_QUERY)


class TestLDAPIModel:

    _URI='http://www.google.com'
    _BATCH=1
    _BATCH_SIZE=10
    _LATITUDE='-39.12345'
    _LONGITUDE='147.12345'
    _ID='http://www.google.com/123'
    _ID_RESULT=(
        '{ "geo" : "<http://www.opengis.net/def/crs/EPSG/0/4283> '
        'POINT(147.12345 -39.12345)" }'
    )
    _GET_RESULT=(
        '{'
            '"views": ["reg","alternates"],'
            '"label": "Address Register",'
            '"default_view": "reg",'
            '"register_items": ['
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845933",'
                    '"Address ID: GAACT714845933",'
                    '"GAACT714845933"'
                '],'
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845934",'
                    '"Address ID: GAACT714845934",'
                    '"GAACT714845934"'
                '],'
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845935",'
                    '"Address ID: GAACT714845935",'
                    '"GAACT714845935"'
                '],'
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845936",'
                    '"Address ID: GAACT714845936",'
                    '"GAACT714845936"'
                '],'
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845938",'
                    '"Address ID: GAACT714845938",'
                    '"GAACT714845938"'
                '],'
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845939",'
                    '"Address ID: GAACT714845939",'
                    '"GAACT714845939"'
                '],'
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845941",'
                    '"Address ID: GAACT714845941",'
                    '"GAACT714845941"'
                '],'
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845942",'
                    '"Address ID: GAACT714845942",'
                    '"GAACT714845942"'
                '],'
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845943",'
                    '"Address ID: GAACT714845943",'
                    '"GAACT714845943"'
                '],'
                '['
                    '"http://linked.data.gov.au/dataset/gnaf/address/GAACT714845944",'
                    '"Address ID: GAACT714845944",'
                    '"GAACT714845944"'
                ']'
            '],'
            '"comment": "Register of all GNAF Addresses",'
            '"contained_item_classes": ["http://linked.data.gov.au/def/gnaf#Address"],'
            '"uri": "http://linked.data.gov.au/dataset/gnaf/address/"'
        '}'
    )

    @pytest.fixture(scope="class")
    def api_model(self):
        api = LDAPIModel(endpoint=self._URI)
        yield api

    @pytest.mark.parametrize("links, more", [
        (['next', 'prev', 'first', 'last'], True),
        (['prev', 'first', 'last'], False)
    ])
    @patch('requests.get')
    def test_get_ids(
            self, mock_get, links, more, api_model
    ):
        mock_get.return_value.text = self._GET_RESULT
        mock_get.return_value.links = links
        rows, more_records = api_model.get_ids(
            batch=self._BATCH, batch_size=self._BATCH_SIZE
        )
        assert more == more_records
        mock_get.side_effect = requests.exceptions.ConnectionError
        with pytest.raises(joiner.FetchIdBatchError):
            rows, more_records = api_model.get_ids(
                batch=self._BATCH, batch_size=self._BATCH_SIZE
            )

    @patch('requests.get')
    def test_get_point(
            self, mock_get, api_model
    ):
        mock_get.return_value.text = self._ID_RESULT
        x, y = api_model.get_point(id=self._ID)
        assert x == self._LONGITUDE
        assert y == self._LATITUDE
        mock_get.side_effect = requests.exceptions.ConnectionError
        with pytest.raises(joiner.FetchPointError):
            x, y = api_model.get_point(id=self._ID)
