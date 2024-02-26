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
        total_age_columns = sum(avg_columns_age)
        num_age_columns = len(avg_columns_age)
        return (
            (total_age_columns / num_age_columns)
            .cast(FloatType())
            .alias("average_age_rate")
        )

    def gender_average(self) -> Column:
        male_rate = col("gender_rate.male_ppltn_rate")
        female_rate = col("gender_rate.female_ppltn_rate")
        return ((male_rate + female_rate) / 2).alias("average_gender_rate")

    def generate_congestion(self, fields: DataFrame, query_type: Column) -> DataFrame:
        grouped_fields = fields.groupBy(
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
        return grouped_fields

    def select_query(self, fields: DataFrame, query_type: str) -> DataFrame:
        if query_type == "gender":
            return self.generate_congestion(fields, self.gender_average())
        elif query_type == "age":
            return self.generate_congestion(fields, self.age_average())
        else:
            raise ValueError(
                "Invalid query type. Supported types are 'gender' and 'age'."
            )
