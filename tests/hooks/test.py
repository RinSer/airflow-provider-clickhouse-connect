"""
Run test:

    python3 -m unittest tests.hooks.test.TestClickhouseConnectHook

"""

import logging
import unittest
from unittest import TestCase
from unittest.mock import patch, MagicMock
import httpretty

from airflow.models import Connection
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.httpclient import HttpClient

# Import Hook
from clickhouse_connect_provider.hooks.clickhouse_connect import ClickhouseConnectHook


log = logging.getLogger(__name__)


def test_connection(conn_id: str):
    return Connection(conn_id=conn_id, host="test")

MockHttpClient = MagicMock()


@patch("airflow.models.Connection.get_connection_from_secrets", new=test_connection)
@patch("clickhouse_connect.driver.httpclient.HttpClient", new=MockHttpClient)
class TestClickhouseConnectHook(TestCase):
    """
    Test Clickhouse Connect Hook.
    """

    @httpretty.activate(verbose=True, allow_net_connect=True)    
    def test_query(self):
        hook = ClickhouseConnectHook()
        
        self.assertEqual(hook.connection_id, ClickhouseConnectHook.default_conn_name)
        
        httpretty.register_uri(
            httpretty.POST,
            "https://test:8123/?wait_end_of_query=1",
            body="23.3.19.32\tEurope/Moscow\n",
            headers={
                "Transfer-Encoding": "chunked",
            }
        )
        
        hook.query("SELECT * FROM test WHERE id = {id:Int32}", {"id": 13})

        
if __name__ == '__main__':
    unittest.main()
