from __future__ import annotations

from enum import Enum
from typing import Any
from typing import Dict
from typing import Iterable
from typing import TYPE_CHECKING

from airflow.models import BaseOperator

from clickhouse_connect_provider.hooks.clickhouse import ClickhouseConnectHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ActionType(Enum):
    """ClickhouseConnectOperator possible actions"""

    QUERY = 1
    COMMAND = 2
    INSERT = 3


class UknownActionTypeError(Exception):
    def __init__(self, action):
        actions = ", ".join(ActionType.__dict__.keys())
        Exception.__init__(
            self,
            f"Unknown action: '{action}'. Should be one of {actions}",
        )


class ClickhouseConnectOperator(BaseOperator):
    """
    Execute SQL queries in Clickhouse.

    :param action: Database action performed
    :type action: ActionType.QUERY or ActionType.COMMAND or ActionType.INSERT
    :param sql: Query text
    :type sql: str
    :param data: Database request parameters or inserted rows as column value tuples
    :type data: Any
    :param database: Database name
    :type database: str | None
    :param connection_id: Database connection ID
    :type connection_id: str | None
    :param settings: Query settings
    :type settings: Dict[str, Any] | None
    :param column_names: Corresponding column names
    :type column_names: str | Iterable[str] | None
    """

    def __init__(
        self,
        action: ActionType,
        sql: str,
        data: Any = None,
        database: str = None,
        connection_id: str = None,
        settings: Dict[str, Any] | None = None,
        column_names: str | Iterable[str] = "*",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.action = action
        self.sql = sql
        self.data = data
        self.database = database
        self.connection_id = connection_id
        self.settings = settings
        self.column_names = column_names

    def execute(self, context: Context) -> Any:
        hook = ClickhouseConnectHook(self.connection_id)

        self.log.info(f"Executing {self.action}: {self.sql}")

        if isinstance(self.action, str):
            try:
                self.action = ActionType.__dict__[self.action]
            except KeyError:
                raise UknownActionTypeError(self.action)

        match self.action:
            case ActionType.QUERY:
                return hook.query(
                    self.sql,
                    database=self.database,
                    params=self.data,
                    settings=self.settings,
                )
            case ActionType.COMMAND:
                return hook.command(
                    self.sql,
                    database=self.database,
                    params=self.data,
                    settings=self.settings,
                )
            case ActionType.INSERT:
                return hook.insert(
                    self.sql,
                    data=self.data,
                    database=self.database,
                    settings=self.settings,
                    column_names=self.column_names,
                )
