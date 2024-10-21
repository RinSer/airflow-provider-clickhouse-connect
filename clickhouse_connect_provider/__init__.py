__version__ = "1.0.0"

## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "airflow-provider-clickhouse-connect",  # Required
        "name": "ClickhouseConnect",  # Required
        "description": "A provider to interact with Clickhouse db",  # Required
        "connection-types": [
            {
                "connection-type": "clickhouse-connect",
                "hook-class-name": "sample_provider.hooks.sample.ClickhouseConnectHook"
            }
        ],
        "versions": [__version__],  # Required
    }
