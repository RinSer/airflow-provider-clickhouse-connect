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
            sql="SELECT * FROM test_query WHERE id = {id:Int32}",
            data={"id": 1},
            settings={"session_id": 1},
            task_id="QUERY_TEST",
        )
        res = op.execute(context={})
        self.assertEqual(1, res.row_count)
        self.assertEqual(("test",), res.column_names)
        self.assertEqual((24,), res.first_row)


if __name__ == "__main__":
    unittest.main()
