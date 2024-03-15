"""
Spark streaming coin average price 
"""

from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import from_json, col, to_json, struct, split, udf, explode

from schema.udf_util import streaming_preprocessing
from schema.data_constructure import (
    average_schema,
    final_schema,
    average_price_chema,
    socket_schema,
    y_age_congestion_schema,
    market_schema,
)
from schema.abstruct_class import AbstructSparkSettingOrganization
from schema.congestion_query import SparkStreamingQueryOrganization as SparkStructQuery

from config.properties import (
    KAFKA_BOOTSTRAP_SERVERS,
    SPARK_PACKAGE,
    COIN_MYSQL_URL,
    COIN_MYSQL_USER,
    COIN_MYSQL_PASSWORD,
    CONGESTION_MYSQL_URL,
    CONGESTION_MYSQL_USER,
    CONGESTION_MYSQL_PASSWORD,
)


class _ConcreteSparkSettingOrganization(AbstructSparkSettingOrganization):
    """SparkSession Setting 모음"""

    def __init__(self, name: str, retrieve_topic: str) -> None:
        """생성자

        Args:
            topics (str): 토픽
            retrieve_topic (str): 처리 후 다시 카프카로 보낼 토픽
        """
        self.name = name
        self.retrieve_topic = retrieve_topic
        self._spark: SparkSession = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """
        Spark Session Args:
            - spark.jars.packages : 패키지
                - 2024년 2월 4일 기준 : Kafka-connect, mysql-connector
            - spark.streaming.stopGracefullyOnShutdown : 우아하게 종료 처리
            - spark.streaming.backpressure.enabled : 유압 밸브
            - spark.streaming.kafka.consumer.config.auto.offset.reset : kafka 스트리밍 경우 오프셋이 없을때 최신 메시지 부터 처리
            - spark.sql.adaptive.enabled : SQL 실행 계획 최적화
            - spark.executor.memory : Excutor 할당되는 메모리 크기를 설정
            - spark.executor.cores : Excutor 할당되는 코어 수 설정
            - spark.cores.max : Spark 에서 사용할 수 있는 최대 코어 수
        """
        return (
            SparkSession.builder.appName("myAppName")
            .master("local[*]")
            .config("spark.jars.packages", f"{SPARK_PACKAGE}")
            # .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            # .config("spark.kafka.consumer.cache.capacity", "")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.streaming.kafka.consumer.config.auto.offset.reset", "latest")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.cores", "4")
            .config("spark.cores.max", "16")
            .getOrCreate()
        )

    def _topic_to_spark_streaming(self, data_format: DataFrame) -> StreamingQuery:
        """
        Kafka Bootstrap Setting Args:
            - kafka.bootstrap.servers : Broker 설정
            - subscribe : 가져올 토픽 (,기준)
                - ex) "a,b,c,d"
            - startingOffsets: 최신순
            - checkpointLocation: 체크포인트
            - value.serializer: 직렬화 종류
        """
        checkpoint_dir: str = f".checkpoint_{self.name}"

        return (
            data_format.writeStream.outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("topic", self.retrieve_topic)
            .option("checkpointLocation", checkpoint_dir)
            .option("startingOffsets", "earliest")
            .option(
                "value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer",
            )
            .start()
        )


class SparkStreamingCoinAverage(_ConcreteSparkSettingOrganization):
    """
    데이터 처리 클래스
    """

    def __init__(self, name: str, topics: str, retrieve_topic: str) -> None:
        """
        Args:
            coin_name (str): 코인 이름
            topics (str): 토픽
            retrieve_topic (str): 처리 후 다시 카프카로 보낼 토픽
        """
        super().__init__(name, retrieve_topic)
        self.topic = topics
        self._streaming_kafka_session: DataFrame = self._stream_kafka_session()
        self.average_price = udf(streaming_preprocessing, average_schema)

    def _stream_kafka_session(self) -> DataFrame:
        """
        Kafka Bootstrap Setting Args:
            - kafka.bootstrap.servers : Broker 설정
            - subscribe : 가져올 토픽 (,기준)
                - ex) "a,b,c,d"
            - startingOffsets: 최신순
        """

        return (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("subscribe", self.topic)
            .option("startingOffsets", "earliest")
            .load()
        )

    def coin_preprocessing(self) -> DataFrame:
        """데이터 처리 pythonUDF사용"""

        return (
            self._streaming_kafka_session.selectExpr("CAST(value AS STRING)")
            .select(from_json("value", schema=final_schema).alias("crypto"))
            .select(
                split(col("crypto.upbit.market"), "-")[1].alias("name"),
                col("crypto.upbit.data").alias("upbit_price"),
                col("crypto.bithumb.data").alias("bithumb_price"),
                col("crypto.coinone.data").alias("coinone_price"),
                col("crypto.korbit.data").alias("korbit_price"),
            )
            .withColumn(
                "average_price",
                self.average_price(
                    col("name"),
                    col("upbit_price"),
                    col("bithumb_price"),
                    col("coinone_price"),
                    col("korbit_price"),
                ).alias("average_price"),
            )
            .select(to_json(struct(col("average_price"))).alias("value"))
        )

    def socket_preprocessing(self) -> DataFrame:
        """웹소켓 처리"""
        return (
            self._streaming_kafka_session.selectExpr("CAST(value as STRING)")
            .select(from_json("value", schema=socket_schema).alias("crypto"))
            .select(explode(col("crypto")).alias("crypto"))
            .select(
                split(col("crypto.market"), "-")[1].alias("name"),
                col("crypto.data").alias("price_data"),
            )
            .withColumn(
                "socket_average_price",
                self.average_price(col("name"), col("price_data")).alias(
                    "socket_average_price"
                ),
            )
            .select(to_json(struct(col("socket_average_price"))).alias("value"))
        )

    def saving_to_mysql_query(self) -> DataFrame:
        """데이터 처리 pythonUDF사용"""

        data_df: DataFrame = self.coin_preprocessing()
        return data_df.select(
            from_json("value", average_price_chema).alias("value")
        ).select(
            col("value.average_price.name").alias("name"),
            col("value.average_price.time").alias("time"),
            col("value.average_price.data.opening_price").alias("opening_price"),
            col("value.average_price.data.max_price").alias("max_price"),
            col("value.average_price.data.min_price").alias("min_price"),
            col("value.average_price.data.prev_closing_price").alias(
                "prev_closing_price"
            ),
            col("value.average_price.data.acc_trade_volume_24h").alias(
                "acc_trade_volume_24h"
            ),
        )

    def _coin_write_to_mysql(
        self, data_format: DataFrame, table_name: str
    ) -> StreamingQuery:
        """
        Function Args:
            - data_format (DataFrame): 저장할 데이터 포맷
            - table_name (str): 체크포인트 저장할 테이블 이름
                - ex) .checkpoint_{table_name}

        MySQL Setting Args (_write_batch_to_mysql):
            - url : JDBC MYSQL connention URL
            - driver : com.mysql.cj.jdbc.Driver
            - dbtable : table
            - user : user
            - password: password
            - mode : append
                - 추가로 들어오면 바로 넣기

        - trigger(processingTime="1 minute")
        """
        checkpoint_dir: str = f".checkpoint_{table_name}"

        def _write_batch_to_mysql(batch_df: DataFrame, batch_id) -> None:
            (
                batch_df.write.format("jdbc")
                .option("url", COIN_MYSQL_URL)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", table_name)
                .option("user", COIN_MYSQL_USER)
                .option("password", COIN_MYSQL_PASSWORD)
                .mode("append")
                .save()
            )

        return (
            data_format.writeStream.outputMode("update")
            .foreachBatch(_write_batch_to_mysql)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="1 minute")
            .start()
        )

    def run_spark_streaming(self) -> None:
        """
        Spark Streaming 실행 함수
        """
        query1 = self._coin_write_to_mysql(
            self.saving_to_mysql_query(), f"table_{self.name}"
        )
        query2 = self._topic_to_spark_streaming(self.socket_preprocessing())

        query1.awaitTermination()
        query2.awaitTermination()
        # query = (
        #     self.socket_preprocessing()
        #     .writeStream.outputMode("update")
        #     .format("console")
        #     .start()
        # )
        # query.awaitTermination()


class SparkStreamingCongestionAverage(_ConcreteSparkSettingOrganization):
    """혼잡도"""

    def __init__(
        self,
        name: str,
        topics: str | list[str],
        retrieve_topic: str,
    ) -> None:
        """생성자

        Args:
            name (str): 이름
            topics (str): 토픽
            retrieve_topic (str): 처리 후 다시 카프카로 보낼 토픽
            schema (SparkDataTypeSchema): spark 타입
        """
        super().__init__(name, retrieve_topic)
        self.topic = topics
        self.name = name

    def _stream_kafka_session(self) -> DataFrame:
        """
        Kafka Bootstrap Setting Args:
            - kafka.bootstrap.servers : Broker 설정
            - subscribe : 가져올 토픽 (,기준)
                - ex) "a,b,c,d"
            - startingOffsets: 최신순
        """

        return (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("subscribe", ",".join(self.topic))
            .option("startingOffsets", "earliest")
            .load()
        )

    def _stream_kafka(self) -> DataFrame:
        """kafka setting wathermark 개선점 필요함"""

        return (
            self._stream_kafka_session()
            .selectExpr("CAST(key as STRING)", "CAST(value as STRING)")
            .select(
                from_json(col("value"), schema=y_age_congestion_schema).alias(
                    "congestion"
                )
            )
            .select("congestion.*")
            .withColumn("ppltn_time", col("ppltn_time").cast("timestamp"))
            .withWatermark("ppltn_time", "1 minute")
        )

    def _congestion_write_to_mysql(
        self, data_format: DataFrame, table_name: str
    ) -> StreamingQuery:
        """
        Function Args:
            - data_format (DataFrame): 저장할 데이터 포맷
            - table_name (str): 체크포인트 저장할 테이블 이름
                - ex) .checkpoint_{table_name}

        MySQL Setting Args (_write_batch_to_mysql):
            - url : JDBC MYSQL connention URL
            - driver : com.mysql.cj.jdbc.Driver
            - dbtable : table
            - user : user
            - password: password
            - mode : append
                - 추가로 들어오면 바로 넣기

        - trigger(processingTime="1 minute")
        """
        checkpoint_dir: str = f".checkpoint_{table_name}"

        def _write_batch_to_mysql(batch_df: DataFrame, batch_id) -> None:
            (
                batch_df.write.format("jdbc")
                .option("url", CONGESTION_MYSQL_URL)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", table_name)
                .option("user", CONGESTION_MYSQL_USER)
                .option("password", CONGESTION_MYSQL_PASSWORD)
                .mode("append")
                .save()
            )

        return (
            data_format.writeStream.outputMode("update")
            .foreachBatch(_write_batch_to_mysql)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="1 minute")
            .start()
        )

    def process(self) -> None:
        """
        최종으로 모든 처리 프로세스를 처리하는 프로세스 함수 시작점
        """
        congestion_df: DataFrame = self._stream_kafka()
        congestion_df.printSchema()
        data = SparkStructQuery().select_query(congestion_df)

        json_df = data.withColumn("value", to_json(struct("*")))

        # # Write to Kafka and Mysql
        query_kafka = self._topic_to_spark_streaming(json_df)
        query_mysql = self._congestion_write_to_mysql(data, f"table_{self.name}")

        query_kafka.awaitTermination()
        query_mysql.awaitTermination()
