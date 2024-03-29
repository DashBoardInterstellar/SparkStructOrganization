"""
spark 
"""

from concurrent.futures import ThreadPoolExecutor
from streaming_connection import (
    SparkStreamingCoinAverage,
    SparkStreamingCongestionAverage,
)
from config.properties import (
    BTC_TOPIC_NAME,
    BTC_AVERAGE_TOPIC_NAME,
    UPBIT_BTC_REAL_TOPIC_NAME,
    ETH_TOPIC_NAME,
    ETH_AVERAGE_TOPIC_NAME,
    AVG_AGE_TOPIC,
)

from schema.topic_list import age_topic_list


def run_spark_streaming1(
    coin_name: str, topics: str, retrieve_topic: str, type_
) -> None:
    SparkStreamingCoinAverage(
        coin_name, topics, retrieve_topic, type_
    ).run_spark_streaming()


def run_spark_streaming2(
    coin_name: str, topics: str, retrieve_topic: str, type_
) -> None:
    SparkStreamingCoinAverage(
        coin_name, topics, retrieve_topic, type_
    ).run_spark_streaming()


def run_spark_streaming3(
    congestion_name: str,
    topics: list,
    retrieve_topic: str,
) -> None:
    SparkStreamingCongestionAverage(congestion_name, topics, retrieve_topic).process()


def spark_in_start() -> None:
    """
    multi-Threading in SPARK application
    """
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(
            run_spark_streaming1,
            "BTC",
            UPBIT_BTC_REAL_TOPIC_NAME,
            BTC_AVERAGE_TOPIC_NAME,
            "socket",
        )
        executor.submit(
            run_spark_streaming2,
            "ETH",
            ETH_TOPIC_NAME,
            ETH_AVERAGE_TOPIC_NAME,
            "socket",
        )
        executor.submit(
            run_spark_streaming3,
            "age",
            age_topic_list,
            AVG_AGE_TOPIC,
        )


if __name__ == "__main__":
    spark_in_start()
