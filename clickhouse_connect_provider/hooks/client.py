from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

import clickhouse_connect
from airflow.hooks.base import BaseHook
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.httpclient import HttpClient
from clickhouse_connect.driver.httputil import get_pool_manager


class ClickhouseHook(BaseHook):
    """
    Clickhouse Connect Hook to interact with Clickhouse db.

    :param connection_id: the ID of Connection configured in UI
    :type connection_id: str
    """

    conn_name_attr = "clickhouse_conn_id"
    default_conn_name = "clickhouse_default"
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
        database: Optional[str] | None = None,
        **kwargs,
    ) -> Client:
        """
        Returns Clickhouse Connect Client.

        :param connection_id: DB connection ID
        :type connection_id: dict
        :param database: SQL query database
        :type database: Optional[str] | None
        :param kwargs: Other connection related settings
        :type kwargs: Dict[str, str]
        """
        conn = self.get_connection(connection_id or self.connection_id)
        kwargs["pool_mgr"] = get_pool_manager(num_pools=1)
        client = clickhouse_connect.get_client(
            host=conn.host,
            username=conn.login,
            password=conn.password,
            database=database or conn.schema,
            port=conn.port,
            **kwargs,
        )
        if isinstance(client, HttpClient):
            client._owns_pool_manager = True
        return client

    def test_connection(self) -> Tuple[bool, str]:
        """Test a connection"""
        client = self.get_conn()
        try:
            client.command(cmd="SELECT version()")
            return True, "Clickhouse connection successfully tested"
        except Exception as e:
            return False, str(e)
        finally:
            client.close()

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": [],
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
