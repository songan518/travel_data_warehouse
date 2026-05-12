"""
dws层城市季度热度主题表脚本
功能：
 1. 关联dwd层表
 2. 提取季度
 3. 计算基础热度指标
 4. 计算游客特征指标
 5. 计算排名指标
 6. 计算环比变化指标
 7. 合并写入dws表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder. \
        appName("dws_city_travel_quarter"). \
        master("local[*]"). \
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://your_server:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        config("spark.sql.catalogImplementation", "hive"). \
        getOrCreate()

#1. 关联表 取出城市字段
df_travel = spark.table("dwd.dwd_user_travel_sequence_info")
df_poi = spark.table("dwd.dwd_poi_info").select(
    "poi_id", "name", "city", "region"
)
df_joined = df_travel.join(
    F.broadcast(df_poi),
    on = "poi_id",
    how = "inner"
)

#2. 提取季度
df_joined = df_joined.withColumn(
    "travel_quarter",
    F.ceil(F.col("travel_month") / 3)
)

df_joined = df_joined.cache()

#3. 计算基础指标
df_agg = df_joined.groupby(
    "travel_year", "travel_quarter", "city", "region"
).agg(
    #热度指标
    F.countDistinct("blog_id").alias("visit_user_cnt"),
    F.count("trip_id").alias("visit_trip_cnt"),
    F.count("*").alias("visit_spot_cnt"),
    F.countDistinct("poi_id").alias("unique_attraction_cnt"),

    #游客特征
    F.sum(
        F.when(F.col("travel_partners") == "Solo", 1).otherwise(0)
    ).alias("solo_trip_cnt"),
)

#4. 游客特征
df_agg = df_agg.withColumn(
    "avg_spot_per_user",
    F.when(F.col("visit_user_cnt") > 0,
        F.floor(F.col("visit_spot_cnt") / F.col("visit_user_cnt"))
    ).otherwise(0)
)

df_agg = df_agg.withColumn(
    "solo_user_ratio",
    F.when(F.col("visit_trip_cnt") > 0,
        F.round(F.col("solo_trip_cnt") / F.col("visit_trip_cnt"), 4)
    ).otherwise(0)
)

#5. 排名指标
df_agg = df_agg.withColumn(
    "user_rank_in_region",
    F.row_number().over(Window.partitionBy("travel_year", "travel_quarter", "region").orderBy(F.col("visit_trip_cnt").desc()))
)

df_agg = df_agg.withColumn(
    "user_rank_overall",
    F.row_number().over(Window.partitionBy("travel_year", "travel_quarter").orderBy(F.col("visit_trip_cnt").desc()))
)

#6. 环比变化指标
df_base_temp = df_agg.select(
    "city", "travel_year", "travel_quarter", "visit_trip_cnt"
)

window_lag_tmp = Window.partitionBy("city").orderBy("travel_year", "travel_quarter")

df_with_prev = df_base_temp.\
    withColumn(
    "prev_visit_trip_cnt",
    F.lag("visit_trip_cnt",1).over(window_lag_tmp)
).\
    withColumn(
    "user_cnt_growth_rate",
    F.when(
        (F.col("prev_visit_trip_cnt").isNotNull()) & (F.col("prev_visit_trip_cnt") > 0),
        F.round((F.col("visit_trip_cnt") - F.col("prev_visit_trip_cnt")) / F.col("prev_visit_trip_cnt"), 4)
    ).otherwise(0)
).\
    withColumn(
    "is_hot_rising",
    F.when(F.col("prev_visit_trip_cnt").isNull(), "无上期数据").\
    when(F.col("user_cnt_growth_rate") > 0.3, "是").\
    otherwise("否")
).select(
    "city", "travel_year", "travel_quarter", "is_hot_rising","user_cnt_growth_rate"
)

#7. 合并写入
df_final = df_agg.join(
    df_with_prev,
    on = ["city", "travel_year", "travel_quarter"],
    how = "left"
).select(
    "city", "region",
    "visit_user_cnt", "visit_trip_cnt", "visit_spot_cnt", "unique_attraction_cnt",
    "avg_spot_per_user", "solo_user_ratio",
    "user_rank_in_region", "user_rank_overall",
    "user_cnt_growth_rate", "is_hot_rising",
    "travel_year", "travel_quarter"
)

df_final.write.mode("overwrite").partitionBy("travel_year", "travel_quarter").saveAsTable("dws.dws_city_travel_quarter")
df_joined.unpersist()
spark.stop()
