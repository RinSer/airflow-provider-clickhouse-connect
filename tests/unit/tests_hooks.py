import unittest

from clickhouse_connect_provider.hooks.client import ClickhouseHook
from tests.unit.base_unit_test import BaseClickhouseConnectTest


class TestUnitClickhouseHook(BaseClickhouseConnectTest):
    """
    Test Clickhouse Connect Hook.

    Run test:

        python -m unittest tests.unit.tests_hooks.TestUnitClickhouseHook
    """

    def test_connection(self):
        success, msg = self.hook.test_connection()
        self.assertTrue(success)
        self.assertEqual("Clickhouse connection successfully tested", msg)

    def test_query(self):
        res = self.hook.get_conn().query(
            query="SELECT * FROM test_query WHERE id = {id:Int32}",
            parameters={"id": 1},
            settings={"session_id": 1},
        )
        self.assertEqual(1, res.row_count)
        self.assertEqual(("test",), res.column_names)
        self.assertEqual((24,), res.first_row)

    def test_command(self):
        res = self.hook.get_conn().command(
            cmd="DELETE FROM test_query WHERE id = 2",
            settings={"session_id": 2},
        )
        self.assertEqual(0, res.written_rows)
        self.assertEqual("0", res.summary["result_bytes"])

    def test_insert(self):
        res = self.hook.get_conn().insert(
            table="test_query",
            data=[(1,), (2,)],
            column_names=["id"],
            settings={"session_id": 3},
        )
        self.assertEqual(2, res.written_rows)

    def setUp(self):
        super().setUp()

        self.hook = ClickhouseHook()
        self.assertEqual(self.hook.connection_id, ClickhouseHook.default_conn_name)


if __name__ == "__main__":
    unittest.main()
