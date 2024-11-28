from __future__ import annotations

from typing import Any
from typing import Dict
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from clickhouse_connect.driver.query import QueryResult

from clickhouse_connect_provider.hooks.client import ClickhouseHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ClickhouseQueryOperator(BaseOperator):
    """
    Execute SQL queries in Clickhouse.

    :param sql: Query text
    :type sql: str
    :param data: Query parameters
    :type data: Any
    :param database: Database name
    :type database: str | None
    :param connection_id: Database connection ID
    :type connection_id: str | None
    :param settings: Query settings
    :type settings: Dict[str, Any] | None
    """

    def __init__(
        self,
        sql: str,
        data: Any = None,
        database: str = None,
        connection_id: str = None,
        settings: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.data = data
        self.database = database
        self.connection_id = connection_id
        self.settings = settings

    def execute(self, context: Context) -> QueryResult:
        hook = ClickhouseHook(self.connection_id)
        client = hook.get_conn(database=self.database)
        try:
            self.log.info(f"Executing: {self.sql}")

            return client.query(
                query=self.sql,
                parameters=self.data,
                settings=self.settings,
            )
        finally:
            client.close()
