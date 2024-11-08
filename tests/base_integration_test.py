from unittest import TestCase
from unittest.mock import patch

from airflow.models import Connection

from clickhouse_provider.hooks.client import ClickhouseHook


CONN_ID = "integration_tests"
TEST_DB = "clickhouse_connect_provider_test"
TEST_TABLE = "test"
TEST_ROW_COUNT = 25_000


class BaseClickhouseIntegrationTest(TestCase):
    """
    Base integration tests fixture for Hook and Operator
    """

    def cleanDb(self):
        self.hook.get_conn().command(f"DROP DATABASE IF EXISTS {TEST_DB}")
        self._conn.stop()

    def setUp(self):
        self._conn = patch(
            "airflow.models.Connection.get_connection_from_secrets",
            new=lambda conn_id: Connection(conn_id=conn_id, host="localhost"),
        )
        self._conn.start()

        self.hook = ClickhouseHook(CONN_ID)
        self.assertEqual(self.hook.connection_id, CONN_ID)

        self.addClassCleanup(self.cleanDb)

        conn = self.hook.get_conn()
        conn.command(f"CREATE DATABASE IF NOT EXISTS {TEST_DB}")
        conn.close()

        conn = self.hook.get_conn(database=TEST_DB)
        conn.command(
            f"CREATE TABLE IF NOT EXISTS {TEST_TABLE} (col1 Int32, col2 String) ORDER BY tuple()"
        )
        conn.insert(TEST_TABLE, [(i, f"Row {i}") for i in range(1, TEST_ROW_COUNT + 1)])
        cnt = conn.query(f"SELECT count() FROM {TEST_TABLE}")
        conn.close()
        self.assertLessEqual(TEST_ROW_COUNT, cnt.first_row[0])
