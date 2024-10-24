from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Iterable
from typing import Sequence
from typing import Tuple

import clickhouse_connect
from airflow.hooks.base import BaseHook
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.client import QueryResult
from clickhouse_connect.driver.client import QuerySummary


class ClickhouseConnectHook(BaseHook):
    """
    Clickhouse Connect Hook to interact with Clickhouse db.

    :param connection_id: the ID of Connection configured in UI
    :type connection_id: str
    """

    conn_name_attr = "clickhouse_conn_id"
    default_conn_name = "clickhouse_connect_default"
    conn_type = "clickhouse-connect"
    hook_name = "ClickhouseConnect"

    def __init__(
        self,
        clickhouse_conn_id: str = default_conn_name,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.connection_id = clickhouse_conn_id

    def get_conn(
        self,
        connection_id: str | None = None,
        database: str | None = None,
        send_receive_timeout: int = 300,
    ) -> Client:
        """
        Returns Clickhouse Connect Client.

        :param connection_id: DB connection ID
        :type connection_id: dict
        :param send_receive_timeout: Connection timeout
        :type send_receive_timeout: int
        """
        conn = self.get_connection(connection_id or self.connection_id)
        return clickhouse_connect.get_client(
            host=conn.host,
            username=conn.login,
            password=conn.password,
            database=database or conn.schema,
            port=conn.port,
            send_receive_timeout=send_receive_timeout,
        )

    def query(
        self,
        sql: str,
        database: str | None = None,
        params: Sequence | Dict[str, Any] | None = None,
        settings: Dict[str, Any] | None = None,
    ) -> QueryResult:
        """
        Performs the SQL query with optional parameters

        :param sql: SQL query text
        :type sql: str
        :param database: SQL query database
        :type database: str | None
        :param params: Query parameters
        :type params: Sequence | Dict[str, Any] | None
        :param settings: Query settings
        :type settings: Dict[str, Any] | None
        """
        return self.get_conn(database=database).query(
            sql, parameters=params, settings=settings
        )

    def command(
        self,
        sql: str,
        database: str | None = None,
        params: Sequence | Dict[str, Any] | None = None,
        settings: Dict[str, Any] | None = None,
    ) -> str | int | Sequence[str] | QuerySummary:
        """
        Performs the SQL command with optional parameters

        :param sql: SQL query text
        :type sql: str
        :param database: SQL query database
        :type database: str | None
        :param params: Query parameters
        :type params: Sequence | Dict[str, Any] | None
        :param settings: Query settings
        :type settings: Dict[str, Any] | None
        """
        return self.get_conn(database=database).command(
            sql, parameters=params, settings=settings
        )

    def insert(
        self,
        table: str,
        data: Sequence[Sequence[Any]],
        database: str | None = None,
        column_names: str | Iterable[str] = "*",
        settings: Dict[str, Any] | None = None,
    ) -> QuerySummary:
        """
        Inserts rows to database

        :param table: Table name
        :type table: str
        :param data: Rows to insert
        :type data: Sequence[Sequence[Any]]
        :param database: SQL query database
        :type database: str | None
        :param column_names: Corresponding column names
        :type column_names: str | Iterable[str] | None
        :param settings: Query settings
        :type settings: Dict[str, Any] | None
        """
        return self.get_conn(database=database).insert(
            table=table, data=data, column_names=column_names, settings=settings
        )

    def test_connection(self) -> Tuple[bool, str]:
        """Test a connection"""
        try:
            self.command("SELECT version()")
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["extra"],
            "relabeling": {
                "host": "Clickhouse Host",
                "schema": "Default Database",
                "port": "Clickhouse HTTP Port",
                "login": "Clickhouse Username",
                "password": "Clickhouse Password",
            },
            "placeholders": {
                "host": "localhost",
                "schema": "default",
                "port": "8123",
                "login": "user",
                "password": "password",
            },
        }
