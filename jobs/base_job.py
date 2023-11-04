from pyspark.sql import SparkSession

import pyspark.sql.types as t

from utils.constants import OUTPUT_DATA_PATH


class TSVData:

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
            nullValue='/N'
        )

    def get_first_rows(self) -> None:
        return self.tsv_df.show(n=10)

    def get_rows_count(self) -> int:
        return self.tsv_df.count()

    def get_schema_info(self) -> t.StructType:
        return self.tsv_df.printSchema()

    def get_all_columns(self) -> list:
        return self.tsv_df.columns

    def get_basic_statistics(self) -> None:
        return self.tsv_df.describe().show()

    def get_basic_statistics_numeric_columns(self) -> None:
        numeric_columns = []

        for field in self.tsv_df.schema.fields:
            if isinstance(field.dataType, t.IntegerType):
                numeric_columns.append(field.name)

        return self.tsv_df.select(numeric_columns).summary().show()

    def write_results_to_file(self) -> None:
        self.tsv_df.write.csv(
            path=OUTPUT_DATA_PATH,
            header=True,
            mode='overwrite'
        )
