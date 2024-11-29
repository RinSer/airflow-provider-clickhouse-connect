import unittest

from clickhouse_provider.operators.query import ClickhouseQueryOperator
from tests.unit.base_unit_test import BaseClickhouseConnectTest


class TestClickhouseOperators(BaseClickhouseConnectTest):
    """
    Test Clickhouse Connect Operator.

    Run test:

        python -m unittest tests.unit.tests_operators.TestClickhouseOperators
    """

    def test_query(self):
        op = ClickhouseQueryOperator(
            sql="SELECT * FROM test_query WHERE id = {id:Int32}",
            data={"id": 1},
            settings={"session_id": 1},
            task_id="QUERY_TEST",
        )
        result = op.execute()
        self.assertEqual((24,), result[0])


if __name__ == "__main__":
    unittest.main()
