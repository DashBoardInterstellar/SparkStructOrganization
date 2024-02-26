from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.functions import col


class SparkStreamingQueryOrganization:
    """
    Spark 동적 쿼리 모음집
    """

    def __init__(self) -> None:
        self.age_columns = [col(f"age_rate.ppltn_rate_{i}") for i in range(10, 80, 10)]
        self.congestion_age_max = F.greatest(*self.age_columns).alias("1st_age")
        self.congestion_age_min = F.least(*self.age_columns).alias("7st_age")

    def _average(self, column: Column) -> Column:
        """
        주어진 DataFrame과 열을 사용하여 평균을 계산하는 내부 도우미 메서드
        """
        return F.avg(column).alias(f"avg_{column._jc.toString()}")

    def _generate_grouped_fields(
        self, fields: DataFrame, avg_column: Column, extra_columns: list = []
    ) -> DataFrame:
        """
        주어진 DataFrame과 추가 열을 사용하여 그룹화된 필드를 생성하는 내부 도우미 메서드
        """
        group_by_columns = [
            col("category").alias("category"),
            col("area_name").alias("area_name"),
            F.regexp_replace(col("ppltn_time"), "Z", "").alias("ppltn_time"),
            col("area_congestion_msg").alias("area_congestion_msg"),
            col("male_ppltn_rate").alias("male_ppltn_rate"),
            col("female_ppltn_rate").alias("female_ppltn_rate"),
            *extra_columns,
            avg_column,
        ]
        agg_columns = [
            self._average(col("area_congestion_lvl")),
            self._average(col("area_ppltn_min")),
            self._average(col("area_ppltn_max")),
        ]
        return fields.groupBy(*group_by_columns).agg(*agg_columns)

    def age_average(self) -> Column:
        """
        나이 평균을 계산하는 메서드
        """
        total_age_column = sum(self.age_columns)
        num_age_columns = len(self.age_columns)
        average_age = total_age_column / num_age_columns
        return average_age.alias("average_age_rate")

    def generate_age(self, fields: DataFrame) -> DataFrame:
        """
        나이에 따른 그룹화된 필드를 생성하는 메서드
        """
        return self._generate_grouped_fields(
            fields,
            self.age_average(),
            extra_columns=self.age_columns
            + [self.congestion_age_max, self.congestion_age_min],
        )

    def select_query(self, fields: DataFrame, query_type: str) -> DataFrame:
        """
        주어진 쿼리 유형에 따라 적절한 쿼리를 선택하는 메서드
        """
        if query_type == "age":
            return self.generate_age(fields)
        else:
            raise ValueError(
                f"Invalid query type '{query_type}'. Supported types are 'gender' and 'age'."
            )
