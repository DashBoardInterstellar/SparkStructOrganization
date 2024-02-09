"""
pyspark data schema
{
    "upbit": {
        "name": "upbit-ETH",
        "timestamp": 1689633864.89345,
        "data": {
            "opening_price": 2455000.0,
            "max_price": 2462000.0,
            "min_price": 2431000.0,
            "prev_closing_price": 2455000.0,
            "acc_trade_volume_24h": 11447.92825886,
        }
    },
    .....
}

"""

from pydantic import BaseModel
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    ArrayType,
    IntegerType,
    DoubleType,
    LongType,
)


data_schema = StructType(
    [
        StructField("opening_price", StringType(), True),
        StructField("trade_price", StringType(), True),
        StructField("max_price", StringType(), True),
        StructField("min_price", StringType(), True),
        StructField("prev_closing_price", StringType(), True),
        StructField("acc_trade_volume_24h", StringType(), True),
    ]
)

market_schema = StructType(
    [
        StructField("market", StringType(), True),
        StructField("time", LongType(), True),
        StructField("coin_symbol", StringType(), True),
        StructField("data", data_schema),
    ]
)

final_schema = StructType(
    [
        StructField("upbit", market_schema),
        StructField("bithumb", market_schema),
        StructField("coinone", market_schema),
        StructField("korbit", market_schema),
    ]
)

"""
# 평균값
{
    "name": "ETH",
    "timestamp": 1689633864.89345,
    "data": {
        "opening_price": 2455000.0,
        "max_price": 2462000.0,
        "min_price": 2431000.0,
        "prev_closing_price": 2455000.0,
        "acc_trade_volume_24h": 11447.92825886,
    }
}
"""
average_schema = StructType(
    StructType(
        [
            StructField("name", StringType()),
            StructField("time", LongType()),
            StructField("data", data_schema),
        ]
    ),
)

average_price_chema = StructType(
    [
        StructField(
            "average_price",
            StructType(average_schema),
        )
    ]
)


# Spark UDF Data Schema
class CoinPrice(BaseModel):
    opening_price: float
    closing_price: float
    max_price: float
    min_price: float
    prev_closing_price: float
    acc_trade_volume_24h: float


class AverageCoinPriceData(BaseModel):
    name: str
    time: int
    data: CoinPrice


# -------------------------------------------------------------------------------------------------------#


# -------------------------------------------------
#        seoul congestion common schema
# -------------------------------------------------

common_schema = StructType(
    [
        StructField("category", StringType(), True),
        StructField("area_name", StringType(), True),
        StructField("area_congestion_lvl", IntegerType(), True),
        StructField("ppltn_time", StringType(), True),
        StructField("area_congestion_msg", StringType(), True),
        StructField("area_ppltn_min", IntegerType(), True),
        StructField("area_ppltn_max", IntegerType(), True),
    ]
)

# ---------------------------------------------------
#    seoul congestion fcst_yn register schema
# ---------------------------------------------------ㅉ

fcst_ppltn_schema = ArrayType(
    StructType(
        [
            StructField("fcst_time", DoubleType(), True),
            StructField("fcst_congest_lvl", IntegerType(), True),
            StructField("fcst_ppltn_min", DoubleType(), True),
            StructField("fcst_ppltn_max", DoubleType(), True),
        ]
    )
)

fcst_yn_schema = StructType([StructField("fcst_ppltn", fcst_ppltn_schema, True)])
fcst_yn = StructType([StructField("fcst_yn", fcst_yn_schema, True)])
# ------------------------------------------------------
#    seoul congestion gender rate schema register
# ------------------------------------------------------

gender_rate_schema = StructField(
    "gender_rate",
    StructType(
        [
            StructField("male_ppltn_rate", DoubleType(), True),
            StructField("female_ppltn_rate", DoubleType(), True),
        ]
    ),
    True,
)

# ------------------------------------------------------
#    seoul congestion age rate schema register
# ------------------------------------------------------

age_congestion_specific_schema = StructField(
    "age_rate",
    StructType(
        [
            StructField("ppltn_rate_0", FloatType(), True),
            StructField("ppltn_rate_10", FloatType(), True),
            StructField("ppltn_rate_20", FloatType(), True),
            StructField("ppltn_rate_30", FloatType(), True),
            StructField("ppltn_rate_40", FloatType(), True),
            StructField("ppltn_rate_50", FloatType(), True),
            StructField("ppltn_rate_60", FloatType(), True),
            StructField("ppltn_rate_70", FloatType(), True),
        ]
    ),
    True,
)

# -------------------------------------------------------------
#    seoul congestion  schema register
# -------------------------------------------------------------

n_fcst_yn = StructField("fcst_yn", StringType(), True)

y_age_congestion_schema = StructType(
    common_schema.fields + fcst_yn.fields + [age_congestion_specific_schema]
)

y_gender_congestion_schema = StructType(
    common_schema.fields + fcst_yn.fields + [gender_rate_schema]
)

n_age_congestion_schema = StructType(
    common_schema.fields + [n_fcst_yn, age_congestion_specific_schema]
)

n_gender_congestion_schema = StructType(
    common_schema.fields + [n_fcst_yn, gender_rate_schema]
)
