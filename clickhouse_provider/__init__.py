__version__ = "0.0.10"


# This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "airflow-provider-clickhouse-connect",  # Required
        "name": "Clickhouse Connect",  # Required
        "description": "A provider to interact with Clickhouse db",  # Required
        "connection-types": [
            {
                "connection-type": "clickhouse-connect",
                "hook-class-name": "clickhouse_provider.hooks.client.ClickhouseConnectHook",
            }
        ],
        "versions": [__version__],  # Required
    }
