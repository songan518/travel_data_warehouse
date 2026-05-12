"""
dws层景点热度主题表脚本
功能：
 1. 关联dwd层表
 2. 提取季度
 3. 计算基础热度指标
 4. 计算排名指标
 5. 计算独自出行占比
 6. 计算环比变化指标
 7. 合并写入dws表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder. \
        appName("dws_attraction_travel_quarter"). \
        master("local[*]"). \
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift//your_server:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        config("spark.sql.catalogImplementation", "hive"). \
        getOrCreate()

#1. 关联表
df_travel = spark.table("dwd.dwd_user_travel_sequence_info")
df_poi = spark.table("dwd.dwd_poi_info").select(
    "poi_id", "name", "city", "category", "region"
)
df_joined = df_travel.join(
    F.broadcast(df_poi),
    on = ["poi_id"],
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
    "poi_id","name", "city", "category", "region",
    "travel_year", "travel_quarter"
).agg(
    #热度指标
    F.countDistinct("blog_id").alias("visit_user_cnt"),    #参观过的独立用户数
    F.countDistinct("trip_id").alias("visit_trip_cnt"),    #被游玩的次数
    F.count("*").alias("visit_total_cnt"),    #被游玩的总人次

    #计算solo次数
    F.sum(
        F.when(F.col("travel_partners") == "Solo", 1).otherwise(0)
    ).alias("solo_trip_cnt")
)

#4. 计算排名指标
  #城市内排名
window_city_tmp = Window.partitionBy(
    "city", "travel_year", "travel_quarter"
).orderBy(F.col("visit_trip_cnt").desc())

df_agg = df_agg.withColumn(
    "user_rank_in_city",
    F.row_number().over(window_city_tmp)
)

  #区域内排名
window_region_tmp = Window.partitionBy(
    "region", "travel_year", "travel_quarter"
).orderBy(F.col("visit_trip_cnt").desc())

df_agg = df_agg.withColumn(
    "user_rank_in_region",
    F.row_number().over(window_region_tmp)
)

  #全量热度排名
window_overall_tmp = Window.partitionBy(
 "travel_year", "travel_quarter"
).orderBy(F.col("visit_trip_cnt").desc())

df_agg = df_agg.withColumn(
    "user_rank_overall",
    F.row_number().over(window_overall_tmp)
)

#5. 游客画像指标  独自出行用户占比
df_agg = df_agg.withColumn(
    "solo_user_ratio",
    F.when(F.col("visit_trip_cnt") > 0,
        F.round(F.col("solo_trip_cnt") / F.col("visit_trip_cnt"), 4)
    ).otherwise(0)
)

#6. 环比变化指标
  #提取需要算环比的数据
df_base_tmp = df_agg.select(
    "poi_id", "travel_year", "travel_quarter", "visit_user_cnt"
)

  #按景点分类
window_lag_tmp = Window.partitionBy("poi_id").orderBy(
    "travel_year", "travel_quarter"
)

  #计算环比增长率
df_with_prev = df_base_tmp.\
    withColumn(
    "prev_visit_user_cnt",
    F.lag("visit_user_cnt", 1).over(window_lag_tmp)
). \
    withColumn(
    "user_cnt_growth_rate",
    F.when(
        (F.col("prev_visit_user_cnt").isNotNull()) & (F.col("prev_visit_user_cnt") > 0),
        F.round(
            (F.col("visit_user_cnt") - F.col("prev_visit_user_cnt"))
            / F.col("prev_visit_user_cnt"), 4
        )
    ).otherwise(0)  # 上期数据为0或NULL时增长率为0
).\
    withColumn(
    "is_hot_rising",
    F.when(F.col("prev_visit_user_cnt").isNull(), "无上期数据")
    .when(F.col("user_cnt_growth_rate") > 0.3, "是")
    .otherwise("否")
).select(
    "poi_id", "travel_year", "travel_quarter",
    "user_cnt_growth_rate", "is_hot_rising"
)

#7. 合并写入
df_final = df_agg.join(
    df_with_prev,
    on=["poi_id", "travel_year", "travel_quarter"],
    how="left"
).select(
    "poi_id", "name", "city", "category", "region",
    "visit_user_cnt", "visit_trip_cnt", "visit_total_cnt",
    "user_rank_in_city", "user_rank_in_region", "user_rank_overall",
    "solo_user_ratio", "user_cnt_growth_rate", "is_hot_rising",
    "travel_year", "travel_quarter"
)

#写入hive
df_final.write.mode("overwrite").partitionBy(
    "travel_year", "travel_quarter"
).saveAsTable("dws.dws_attraction_travel_quarter")

df_joined.unpersist()
spark.stop()
