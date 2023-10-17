from typing import Any
from dagster import (
    AssetKey,
    IOManager,
    io_manager,
    InputContext,
    OutputContext,
    InitResourceContext,
    StringSource,
)
import pandas as pd
from sqlalchemy import create_engine


class PostgresDataframeIOManager(IOManager):
    def __init__(self, uid: str, pwd: str, server: str, port: str, db: str) -> None:
        self.uid = uid
        self.pwd = pwd
        self.server = server
        self.port = port
        self.db = db

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return

        table_name = context.asset_key.to_python_identifier()
        engine = create_engine(
            f"postgresql://{self.uid}:{self.pwd}@{self.server}:{self.port}/{self.db}"
        )
        obj.to_sql(table_name, con=engine, if_exists="replace", index=False)

        context.add_output_metadata({"db": self.db, "table_name": table_name})

    def load_input(self, context: InputContext) -> pd.DataFrame:
        # upstream_output_key contains the details for asset which loaded the data
        table_name = context.upstream_output.asset_key.to_python_identifier()

        engine = create_engine(
            f"postgresql://{self.uid}:{self.pwd}@{self.server}:{self.port}/{self.db}"
        )
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
        return df


@io_manager(
    config_schema={
        "uid": StringSource,
        "pwd": StringSource,
        "server": StringSource,
        "port": StringSource,
        "db": StringSource,
    }
)
def postgres_pandas_io_manager(
    context: InitResourceContext,
) -> PostgresDataframeIOManager:
    return PostgresDataframeIOManager(
        uid=context.resource_config["uid"],
        pwd=context.resource_config["pwd"],
        server=context.resource_config["server"],
        port=context.resource_config["port"],
        db=context.resource_config["db"],
    )
