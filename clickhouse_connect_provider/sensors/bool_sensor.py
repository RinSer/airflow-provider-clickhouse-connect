from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from clickhouse_connect_provider.hooks.client import ClickhouseHook


class ClickhouseBoolSensor(BaseSensorOperator):
    """
    Executes a query in Clickhouse that returns boolean row
    and returns its value

    :param conn_id: The connection to run the sensor against
    :type conn_id: str
    :param query: Query to execute
    :type query: str
    """

    def __init__(
        self,
        *,
        conn_id: str = ClickhouseHook.default_conn_name,
        query: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.hook = ClickhouseHook(clickhouse_conn_id=conn_id)

    def poke(self, _: Context) -> bool:
        self.log.info(f"Poking: {self.query}")
        result = self.hook.get_conn().query(self.query)
        return result.first_row[0]
