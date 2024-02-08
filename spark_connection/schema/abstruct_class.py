from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class AbstructSparkSettingOrganization(ABC):
    """Abstract class for SparkSession settings"""

    @abstractmethod
    def create_spark_session(self):
        """Abstract method to create SparkSession"""
        pass

    @abstractmethod
    def stream_kafka_session(self):
        """Abstract method to create Kafka streaming session"""
        pass

    @abstractmethod
    def topic_to_spark_streaming(self, data_format: DataFrame):
        """Abstract method to convert topic to Spark streaming"""
        pass

    @abstractmethod
    def write_to_mysql(self, data_format: DataFrame, table_name: str):
        """Abstract method to write to MySQL"""
        pass
