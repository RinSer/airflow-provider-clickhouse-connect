import unittest

from clickhouse_provider.operators.query import ClickhouseQueryOperator
from tests.integration.base_integration_test import BaseClickhouseIntegrationTest
from tests.integration.base_integration_test import CONN_ID
from tests.integration.base_integration_test import TEST_DB
from tests.integration.base_integration_test import TEST_TABLE


class TestIntegrationClickhouseOperator(BaseClickhouseIntegrationTest):
    """
    Test Clickhouse Connect Operator.

    Run test:

        python -m unittest tests.integration.tests_operators.TestIntegrationClickhouseOperator
    """

    def test_query(self):
        row_count = 5000
        op = ClickhouseQueryOperator(
            sql=f"SELECT * FROM {TEST_TABLE} WHERE col1 <= {{col1:Int32}}",
            data={"col1": row_count},
            database=TEST_DB,
            connection_id=CONN_ID,
            settings={"session_id": "test_session"},
            task_id="QUERY_OPERATOR_TEST",
        )
        res = op.execute(context={})
        self.assertEqual(row_count, res.row_count)
        self.assertEqual(("col1", "col2"), res.column_names)
        for i in range(1, row_count + 1):
            self.assertIn((i, f"Row {i}"), res.result_rows)


if __name__ == "__main__":
    unittest.main()
