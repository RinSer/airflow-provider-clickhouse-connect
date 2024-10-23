import unittest
from types import GeneratorType
from typing import Any
from unittest import TestCase
from unittest.mock import patch

from airflow.models import Connection
from urllib3.poolmanager import PoolManager
from urllib3.response import HTTPResponse

from clickhouse_connect_provider.hooks.clickhouse_connect import ClickhouseConnectHook
from tests.urllib3_mock import mockChunckedBody
from tests.urllib3_mock import mockHttpResponse


class TestClickhouseConnectHook(TestCase):
    """
    Test Clickhouse Connect Hook.

    Run test:

        python -m unittest tests.hooks.test.TestClickhouseConnectHook
    """

    def test_query(self):
        res = self.hook.query(
            sql="SELECT * FROM test_query WHERE id = {id:Int32}",
            params={"id": 1},
            settings={"session_id": 1},
        )
        self.assertEqual(1, res.row_count)
        self.assertEqual(("test",), res.column_names)
        self.assertEqual((24,), res.first_row)

    def test_command(self):
        res = self.hook.command(
            sql="DELETE FROM test_query WHERE id = {id:Int32}",
            params={"id": 2},
            settings={"session_id": 2},
        )
        self.assertEqual(0, res.written_rows)
        self.assertEqual("0", res.summary["result_bytes"])

    def test_insert(self):
        res = self.hook.insert(
            table="test_query",
            data=[(1,), (2,)],
            column_names=["id"],
            settings={"session_id": 3},
        )
        self.assertEqual(2, res.written_rows)

    def assert_requests(self):
        def catch_request(
            _: PoolManager,
            method: str,
            url: str,
            **kwargs: Any,
        ) -> HTTPResponse:
            resp = mockHttpResponse(method, url, **kwargs)
            if method == "POST":
                if b"SELECT * FROM test_query" in kwargs.get("body", b""):
                    self.assertIn("param_id=1", url)
                    self.assertIn("session_id=1", url)
                    resp._fp = mockChunckedBody(
                        ["\x01", "\x01", "\x04test", "\x05Int32", "\x18\x00\x00\x00"]
                    )
                elif b"DELETE FROM test_query" in kwargs.get("body", b""):
                    self.assertIn("param_id=2", url)
                    self.assertIn("session_id=2", url)
                    resp.headers["X-ClickHouse-Summary"] = (
                        '{"written_rows":"0","result_bytes":"0"}'
                    )
                elif b"DESCRIBE TABLE `test_query`" in kwargs.get("body", b""):
                    resp._fp = mockChunckedBody(
                        [
                            "\x07",
                            "\x01",
                            "\x04name",
                            "\x06String",
                            "\x02id",
                            "\x04type",
                            "\x06String",
                            "\x05Int32",
                            "\x0cdefault_type",
                            "\x06String",
                            "\x07DEFAULT",
                            "\x12default_expression",
                            "\x06String",
                            "\x011",
                            "\x07comment",
                            "\x06String",
                            "\x02NO",
                            "\x10codec_expression",
                            "\x06String",
                            "\x01X",
                            "\x0ettl_expression",
                            "\x06String",
                            "\x021s",
                        ]
                    )
                elif isinstance(kwargs.get("body", ""), GeneratorType):
                    self.assertIn("session_id=3", url)
                    resp.headers["X-ClickHouse-Summary"] = '{"written_rows":"2"}'
                    resp._fp = mockChunckedBody(["\x00", "\x00"])
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

        self.hook = ClickhouseConnectHook()
        self.assertEqual(
            self.hook.connection_id, ClickhouseConnectHook.default_conn_name
        )


if __name__ == "__main__":
    unittest.main()
