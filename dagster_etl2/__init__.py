from dagster import Definitions, load_assets_from_modules

from . import assets_productcategory
from .io_managers import file_io, db_io

all_assets = load_assets_from_modules([assets_productcategory])

defs = Definitions(
    assets=all_assets,
    resources={
        "file_io": file_io.csv_io_manager(),
        "db_io": db_io.postgres_pandas_io_manager.configured(
            {
                "uid": {"env": "uid"},
                "pwd": {"env": "pwd"},
                "server": {"env": "server"},
                "port": {"env": "port"},
                "db": {"env": "db"},
            }
        ),
    },
)
