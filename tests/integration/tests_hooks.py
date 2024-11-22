import unittest

from tests.integration.base_integration_test import BaseClickhouseIntegrationTest
from tests.integration.base_integration_test import TEST_DB
from tests.integration.base_integration_test import TEST_ROW_COUNT
from tests.integration.base_integration_test import TEST_TABLE


class TestIntegrationClickhouseHook(BaseClickhouseIntegrationTest):
    """
    Test Clickhouse Connect Hook.

    Run test:

        python -m unittest tests.integration.tests_hooks.TestIntegrationClickhouseHook
    """

    def test_connection(self):
        success, msg = self.hook.test_connection()
        self.assertTrue(success)
        self.assertEqual("Clickhouse connection successfully tested", msg)

    def test_query(self):
        res = self.hook.get_conn(database=TEST_DB).query(
            query=f"SELECT * FROM {TEST_TABLE} WHERE col1 > " + "{id:Int32}",
            parameters={"id": TEST_ROW_COUNT // 2},
        )
        self.assertEqual(("col1", "col2"), res.column_names)
        for i in range(TEST_ROW_COUNT // 2 + 1, TEST_ROW_COUNT + 1):
            self.assertIn((i, f"Row {i}"), res.result_rows)

    def test_command(self):
        res = self.hook.get_conn(database=TEST_DB).command(
            cmd=f"DELETE FROM {TEST_TABLE} WHERE col1 < {TEST_ROW_COUNT // 2}",
        )
        self.assertEqual(0, res.written_rows)
        self.assertEqual("0", res.summary["result_bytes"])
        res = self.hook.get_conn(database=TEST_DB).query(
            query=f"SELECT count() FROM {TEST_TABLE}",
        )
        self.assertLess(res.first_row[0], TEST_ROW_COUNT // 2 + 2)

    def test_insert(self):
        test_rows = [
            (TEST_ROW_COUNT + 1, "Test row 1"),
            (TEST_ROW_COUNT + 2, "Test row 2"),
        ]
        res = self.hook.get_conn(database=TEST_DB).insert(
            table=TEST_TABLE,
            data=test_rows,
            column_names=["col1", "col2"],
        )
        self.assertEqual(2, res.written_rows)
        res = self.hook.get_conn(database=TEST_DB).query(
            query=f"SELECT * FROM {TEST_TABLE} WHERE col1 >= " + "{id:Int32}",
            parameters={"id": TEST_ROW_COUNT + 1},
        )
        self.assertEqual(2, res.row_count)
        for row in test_rows:
            self.assertIn(row, res.result_rows)


if __name__ == "__main__":
    unittest.main()
