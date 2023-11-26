"""Module for the parent class of each dataset"""

from pyspark.sql import SparkSession
import pyspark.sql.types as t


class TSVData:
    """Parent class with basic methods"""

    def __init__(
        self,
        spark_session: SparkSession,
        path: str,
        schema: t.StructType
    ) -> None:
        self.tsv_df = spark_session.read.csv(
            path=path,
            schema=schema,
            sep='\t',
            header=True,
            nullValue='\\N'
        )

    def get_first_rows(self) -> None:
        """Get first 10 rows of the dataset"""
        return self.tsv_df.show(n=10)

    def get_rows_count(self) -> int:
        """Get the total number of rows"""
        return self.tsv_df.count()

    def get_schema_info(self) -> None:
        """Get information about the schema"""
        return self.tsv_df.printSchema()

    def get_all_columns(self) -> list:
        """Get the total columns count"""
        return self.tsv_df.columns

    def get_basic_statistics(self) -> None:
        """Get basic statistics of all columns"""
        return self.tsv_df.describe().show()

    def get_basic_statistics_numeric_columns(self) -> None:
        """Get basic statistics for numerical columns only"""
        numeric_columns = []

        for field in self.tsv_df.schema.fields:
            if isinstance(field.dataType, t.IntegerType):
                numeric_columns.append(field.name)

        return self.tsv_df.select(numeric_columns).summary().show()
