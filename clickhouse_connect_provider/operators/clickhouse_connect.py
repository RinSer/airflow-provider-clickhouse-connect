from __future__ import annotations

from enum import Enum
from typing import Any
from typing import TYPE_CHECKING

from airflow.models import BaseOperator

from clickhouse_connect_provider.hooks.clickhouse_connect import ClickhouseConnectHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ActionType(Enum):
    QUERY = 1
    COMMAND = 2
    INSERT = 3


class ClickhouseConnectOperator(BaseOperator):
    """
    Execute SQL queries in Clickhouse.

    :param action: Database action performed
    :type action: ActionType
    :param sql: Query text
    :type sql: str
    :param params: Database request parameters
    :type params: Any
    :param database: Database name
    :type database: str | None
    :param connection_id: Database connection ID
    :type connection_id: str | None
    """

    def __init__(
        self,
        action: ActionType,
        sql: str,
        params: Any = None,
        database: str = None,
        connection_id: str = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.action = action
        self.sql = sql
        self.params = params
        self.database = database
        self.connection_id = connection_id

    def execute(self, _: Context) -> Any:
        hook = ClickhouseConnectHook(self.connection_id)

        self.log.info(f"Executing {self.action}: {self.sql}")

        match self.action:
            case ActionType.QUERY:
                return hook.query(self.sql, database=self.database, params=self.params)
            case ActionType.COMMAND:
                return hook.command(
                    self.sql, database=self.database, params=self.params
                )
            case ActionType.INSERT:
                return hook.insert(self.sql, data=self.params, database=self.database)
