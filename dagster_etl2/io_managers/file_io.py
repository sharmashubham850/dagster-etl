from dagster import AssetKey, IOManager, io_manager, InputContext, OutputContext
import pandas as pd
import os


class LocalFileSystemIOManager(IOManager):
    """Translates between pandas dataframe and CSV in local filesystem"""

    def _get_fs_path(self, asset_key: AssetKey) -> str:
        rpath = os.path.join("warehouse_location/result", *asset_key.path) + ".csv"
        return os.path.abspath(rpath)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """This saves the dataframe as a CSV"""
        fpath = self._get_fs_path(context.asset_key)
        context.add_output_metadata({"file_path": fpath})
        obj.to_csv(fpath)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """This reads a dataframe from a CSV"""
        fpath = self._get_fs_path(context.asset_key)
        return pd.read_csv(fpath)


# Defining our io manager
@io_manager
def csv_io_manager() -> LocalFileSystemIOManager:
    return LocalFileSystemIOManager()
