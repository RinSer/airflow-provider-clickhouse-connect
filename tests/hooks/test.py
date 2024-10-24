import unittest

from clickhouse_connect_provider.hooks.clickhouse import ClickhouseConnectHook
from tests.base_test import BaseClickhouseConnectTest


class TestClickhouseConnectHook(BaseClickhouseConnectTest):
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

    def setUp(self):
        super().setUp()

        self.hook = ClickhouseConnectHook()
        self.assertEqual(
            self.hook.connection_id, ClickhouseConnectHook.default_conn_name
        )


if __name__ == "__main__":
    unittest.main()
