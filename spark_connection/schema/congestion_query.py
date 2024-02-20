"""쿼리 모음집"""

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col


class SparkStreamingQueryOrganization:
    """
    Spark 동적 쿼리 모음집
    """

    def age_average(self) -> Column:
        avg_columns_age: list[Column] = [
            col(f"age_rate.ppltn_rate_{i}").alias(f"avg_ppltn_rate_{i}")
            for i in range(10, 80, 10)
        ]

        return F.lit(
            sum(avg_columns_age).astype(FloatType()).alias("average_rate")
            / len(avg_columns_age)
        ).alias("average_age_rate")

    def gender_average(self) -> Column:
        return F.lit(
            (col("gender_rate.male_ppltn_rate") + col("gender_rate.female_ppltn_rate"))
            / 2
        ).alias("average_gender_rate")

    def sql_for_congestion(self, fields: DataFrame, query_type: Column) -> str:
        return fields.groupBy(
            col("category").alias("category"),
            col("area_name").alias("area_name"),
            F.regexp_replace(col("ppltn_time"), "Z", "").alias("ppltn_time"),
            col("area_congestion_msg").alias("area_congestion_msg"),
            query_type,
        ).agg(
            F.avg(col("area_congestion_lvl")).alias("avg_congestion_lvl"),
            F.avg(col("area_ppltn_min")).alias("avg_ppltn_min"),
            F.avg(col("area_ppltn_max")).alias("avg_ppltn_max"),
        )

    def select_query(self, fields: DataFrame, type_: str) -> DataFrame:
        if type_ == "gender":
            return self.sql_for_congestion(fields, self.gender_average())
        if type_ == "age":
            return self.sql_for_congestion(fields, self.age_average())
