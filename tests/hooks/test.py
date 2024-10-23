import unittest
from typing import Any
from unittest import TestCase
from unittest.mock import patch

from airflow.models import Connection
from urllib3.poolmanager import PoolManager
from urllib3.response import HTTPResponse

from clickhouse_connect_provider.hooks.clickhouse_connect import ClickhouseConnectHook
from tests.urllib3_mock import mockHttpResponse, mockChunckedBody


class TestClickhouseConnectHook(TestCase):
    """
    Test Clickhouse Connect Hook.

    Run test:

        python3 -m unittest tests.hooks.test.TestClickhouseConnectHook
    """

    def test_query(self):
        hook = ClickhouseConnectHook()
        self.assertEqual(hook.connection_id, ClickhouseConnectHook.default_conn_name)

        res = hook.query(
            "SELECT * FROM test_query WHERE id = {id:Int32}", 
            params={"id": 13},
            settings={"session_id": 13}
        )
        self.assertEqual(1, res.row_count)
        self.assertEqual(("test",), res.column_names)
        self.assertEqual((24,), res.first_row)
        
    def assert_requests(self):
        def catch_request(
            _: PoolManager,
            method: str,
            url: str,
            **kwargs: Any,
        ) -> HTTPResponse:
            resp = mockHttpResponse(method, url, **kwargs)
            if method == "POST":
                if b"FROM test_query" in kwargs.get("body", b""):
                    resp._fp = mockChunckedBody(["\x01", "\x01", "\x04test", "\x05Int32", "\x18\x00\x00\x00"])
            return resp
        return catch_request

    def setUp(self):
        self._conn = patch(
            "airflow.models.Connection.get_connection_from_secrets",
            new=lambda conn_id: Connection(conn_id=conn_id, host="test"),
        )
        self.addCleanup(self._conn.stop)
        self._poolmanager = patch(
            "urllib3.poolmanager.PoolManager.request", new=self.assert_requests()
        )
        self.addCleanup(self._poolmanager.stop)
        self._conn.start()
        self._poolmanager.start()


if __name__ == "__main__":
    unittest.main()
