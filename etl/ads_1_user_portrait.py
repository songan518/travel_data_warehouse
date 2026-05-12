"""
ads层-用户画像表
功能：
 1. 读取dws层用户季度宽表
 2. 聚合计算用户全量指标
 3. 计算用户偏好城市与分类
 4. 计算用户活跃等级与出行范围
 5. 写入ads表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("ads_user_portrait").\
        master("local[*]").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://your_server:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        config("spark.sql.catalogImplementation", "hive"). \
        getOrCreate()

#1. 导入数据
df_dws_user = spark.table("dws.dws_user_travel_quarter")

df_city_exploded_tmp = df_dws_user.select(
    "blog_id",
    F.explode("city_visit_map").alias("city", "cnt")
)

df_region_exploded_tmp = df_dws_user.select(
    "blog_id",
    F.explode("region_arr").alias("region")
)

df_cate_exploded_tmp = df_dws_user.select(
    "blog_id",
    F.explode("cate_visit_map").alias("category", "cnt")
)

#2. 聚合 进行基础指标运算
df_user = df_dws_user.groupby("blog_id").agg(
    F.sum("trip_total_cnt").alias("total_trips"),
    F.sum("total_spot_cnt").alias("total_spots"),
    F.countDistinct("travel_year").alias("active_years"),
    F.min("travel_year").alias("first_active_year"),
    F.max("travel_year").alias("last_active_year"),
    F.round(F.avg("avg_spot_per_trip"), 2).alias("avg_spots_per_trip"),
    F.round(F.avg("solo_trip_ratio"),2).alias("solo_trip_ratio")
)

#3. 计算total_cities
df_total_cities = df_city_exploded_tmp.groupby("blog_id").agg(
    F.countDistinct("city").alias("total_cities")
)

df_user = df_user.join(df_total_cities, on=["blog_id"], how="left")

#4. 计算total_regions
df_total_regions = df_region_exploded_tmp.groupby("blog_id").agg(
    F.countDistinct("region").alias("total_regions")
)

df_user = df_user.join(df_total_regions, on=["blog_id"], how="left")

#5. 计算favorite_city
df_city_total_tmp = df_city_exploded_tmp.groupby("blog_id", "city").agg(
    F.sum("cnt").alias("total_cnt")
)

df_favorite_city = df_city_total_tmp.withColumn(
    "rank", F.dense_rank().over(Window.partitionBy("blog_id").orderBy(F.col("total_cnt").desc()))
).\
    filter(F.col("rank") == 1).\
    groupby("blog_id").\
    agg(F.collect_list("city").alias("favorite_city"))

df_user = df_user.join(df_favorite_city, on=["blog_id"], how="left")

#6. 计算favorite_category
df_cate_total_tmp = df_cate_exploded_tmp.groupBy("blog_id", "category").agg(
    F.sum("cnt").alias("total_cnt")
)

df_favorite_cate = df_cate_total_tmp.withColumn(
    "rank", F.dense_rank().over(Window.partitionBy("blog_id").orderBy(F.col("total_cnt").desc()))
).\
    filter(F.col("rank") == 1).\
    groupby("blog_id").\
    agg(F.collect_list("category").alias("favorite_category"))

df_user = df_user.join(df_favorite_cate, on=["blog_id"], how="left")

#7. 计算travel_level
df_user = df_user.withColumn("travel_level",
    F.when(F.col("total_trips") / F.col("active_years") >= 2, "高频")
        .when(F.col("total_trips") / F.col("active_years") >= 1, "中频")
        .otherwise("低频")
)

#8. 计算region_type
df_user = df_user.withColumn("region_type",
    F.when(F.col("total_regions") >= 2, "全国型")
      .when(F.col("total_cities") >= 2, "区域型")
      .otherwise("单城型")
)

#9. 写入表
df_user.select(
    "blog_id",
    "total_trips",
    "total_spots",
    "total_cities",
    "total_regions",
    "active_years",
    "first_active_year",
    "last_active_year",
    "avg_spots_per_trip",
    "solo_trip_ratio",
    "favorite_city",
    "favorite_category",
    "travel_level",
    "region_type"
).write.mode("overwrite").saveAsTable("ads.ads_user_portrait")

spark.stop()
