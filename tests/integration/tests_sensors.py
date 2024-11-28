import unittest

from clickhouse_provider.sensors.bool_sensor import ClickhouseBoolSensor
from tests.integration.base_integration_test import BaseClickhouseIntegrationTest
from tests.integration.base_integration_test import CONN_ID


class TestIntegrationClickhouseBoolSensor(BaseClickhouseIntegrationTest):
    """
    Test Clickhouse Connect Bool Sensor.

    Run test:

        python -m unittest tests.integration.tests_sensors.TestIntegrationClickhouseBoolSensor
    """

    def test_true(self):
        self.assertTrue(
            ClickhouseBoolSensor(
                conn_id=CONN_ID, query="SELECT true", task_id="TEST_TRUE_TASK"
            ).poke({})
        )

    def test_false(self):
        self.assertFalse(
            ClickhouseBoolSensor(
                conn_id=CONN_ID, query="SELECT false", task_id="TEST_FALSE_TASK"
            ).poke({})
        )


if __name__ == "__main__":
    unittest.main()
