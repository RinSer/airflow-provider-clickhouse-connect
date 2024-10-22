import unittest
from typing import Any
from unittest import TestCase
from unittest.mock import patch

from airflow.models import Connection
from urllib3.poolmanager import PoolManager
from urllib3.response import HTTPResponse

from clickhouse_connect_provider.hooks.clickhouse_connect import ClickhouseConnectHook
from tests.urllib3_mock import mockChunckedBody


def catch_request(
    _: PoolManager,
    method: str,
    url: str,
    **kwargs: Any,
) -> HTTPResponse:
    resp = HTTPResponse(
        headers={
            "X-ClickHouse-Summary": "{}",
            "X-ClickHouse-Query-Id": "test_query",
            "transfer-encoding": "chunked",
        },
        status=200,
        version=0,
        version_string="0",
        reason="test",
        preload_content=False,
        decode_content=False,
        request_url=url,
    )
    if method == "POST":
        if kwargs.get("body", "") == b"SELECT version(), timezone()":
            resp._body = b"25\tEurope/Moscow\n"
        elif b"FROM system.settings" in kwargs.get("body", ""):
            resp._fp = mockChunckedBody(["\0", "\0"])
        else:
            resp._fp = mockChunckedBody(["\0", "\0"])
    return resp


class TestClickhouseConnectHook(TestCase):
    """
    Test Clickhouse Connect Hook.

    Run test:

        python3 -m unittest tests.hooks.test.TestClickhouseConnectHook
    """

    def test_query(self):
        hook = ClickhouseConnectHook()
        self.assertEqual(hook.connection_id, ClickhouseConnectHook.default_conn_name)

        res = hook.query("SELECT * FROM test WHERE id = {id:Int32}", {"id": 13})
        self.assertEqual(0, res.row_count)

    def setUp(self):
        self._conn = patch(
            "airflow.models.Connection.get_connection_from_secrets",
            new=lambda conn_id: Connection(conn_id=conn_id, host="test"),
        )
        self.addCleanup(self._conn.stop)
        self._poolmanager = patch(
            "urllib3.poolmanager.PoolManager.request", new=catch_request
        )
        self.addCleanup(self._poolmanager.stop)
        self._conn.start()
        self._poolmanager.start()


if __name__ == "__main__":
    unittest.main()
