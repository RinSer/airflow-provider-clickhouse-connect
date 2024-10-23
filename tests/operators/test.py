import unittest

from clickhouse_connect_provider.operators.clickhouse import (
    ClickhouseConnectOperator,
)
from tests.base_test import BaseClickhouseConnectTest


class TestClickhouseConnectOperator(BaseClickhouseConnectTest):
    """
    Test Clickhouse Connect Operator.

    Run test:

        python -m unittest tests.operators.test.TestClickhouseConnectOperator
    """

    def test_query(self):
        op = ClickhouseConnectOperator(
            action="QUERY",
            sql="SELECT * FROM test_query WHERE id = {id:Int32}",
            data={"id": 1},
            settings={"session_id": 1},
            task_id="QUERY_TEST",
        )
        res = op.execute(context={})
        self.assertEqual(1, res.row_count)
        self.assertEqual(("test",), res.column_names)
        self.assertEqual((24,), res.first_row)

    def test_command(self):
        op = ClickhouseConnectOperator(
            action="COMMAND",
            sql="DELETE FROM test_query WHERE id = {id:Int32}",
            data={"id": 2},
            settings={"session_id": 2},
            task_id="COMMAND_TEST",
        )
        res = op.execute(context={})
        self.assertEqual(0, res.written_rows)
        self.assertEqual("0", res.summary["result_bytes"])

    def test_insert(self):
        op = ClickhouseConnectOperator(
            action="INSERT",
            sql="test_query",
            data=[(1,), (2,)],
            settings={"session_id": 3},
            column_names=["id"],
            task_id="INSERT_TEST",
        )
        res = op.execute(context={})
        self.assertEqual(2, res.written_rows)


if __name__ == "__main__":
    unittest.main()
