from dagster import asset, OpExecutionContext
import pandas as pd
from dagster_etl2.resources.db_conn import get_sql_conn


@asset(group_name="ProductCategory", compute_kind="pandas", io_manager_key="file_io")
def extract_dim_product_category(context: OpExecutionContext) -> pd.DataFrame:
    """Extract data from SQL Server"""

    with get_sql_conn() as conn:
        df = pd.read_sql_query("SELECT * FROM dbo.DimProductCategory", con=conn)
        context.log.info(df.head())
        return df


@asset(group_name="ProductCategory", compute_kind="pandas", io_manager_key="db_io")
def dim_product_category(
    context: OpExecutionContext, extract_dim_product_category: pd.DataFrame
) -> pd.DataFrame:
    "Transform and stage data into Postgres"
    try:
        context.log.info(extract_dim_product_category.head())
        df = extract_dim_product_category[
            ["ProductCategoryKey", "EnglishProductCategoryName"]
        ]
        df = df.rename(columns={"EnglishProductCategoryName": "ProductCategoryName"})
        return df
    except Exception as e:
        context.log.info(str(e))
