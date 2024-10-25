from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

import clickhouse_connect
from airflow.hooks.base import BaseHook
from clickhouse_connect.driver.client import Client


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
        database: Optional[str] | None = None,
        interface: Optional[str] = None,
        **kwargs,
    ) -> Client:
        """
        Returns Clickhouse Connect Client.

        :param connection_id: DB connection ID
        :type connection_id: dict
        :param database: SQL query database
        :type database: Optional[str] | None
        :param interface: Defaults to http, or to https if port is set to 8443 or 443
        :type interface: Dict[str, str]
        :param kwargs: Other connection related settings
        :type kwargs: Dict[str, str]
        """
        conn = self.get_connection(connection_id or self.connection_id)
        return clickhouse_connect.get_client(
            host=conn.host,
            username=conn.login,
            password=conn.password,
            database=database or conn.schema,
            interface=interface,
            port=conn.port,
            **kwargs,
        )

    def test_connection(self) -> Tuple[bool, str]:
        """Test a connection"""
        try:
            self.command("SELECT version()")
            return True, "Clickhouse connection successfully tested"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import Select2Widget
        from flask_babel import lazy_gettext
        from wtforms import SelectField

        return {
            "interface": SelectField(
                lazy_gettext("Interface"),
                choices=["HTTP", "HTTPS"],
                widget=Select2Widget(),
            ),
        }

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
                "interface": "HTTP or HTTPS interface",
            },
            "placeholders": {
                "host": "localhost",
                "schema": "default",
                "port": "8123",
                "login": "user",
                "password": "password",
                "interface": "HTTP or HTTPS",
            },
        }
