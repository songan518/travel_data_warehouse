"""
dws层用户季度出行宽表脚本
功能：
 1. 关联dwd层表
 2. 提取季度
 3. 基础分组聚合
 4. 计算跨区域次数
 5. 计算偏好指标
 6. 计算出行最多月份
 7. 合并所有指标
 8. 写入dws表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("dws_user_travel_quarter").\
        master("local[*]").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://your_server:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        config("spark.sql.catalogImplementation", "hive"). \
        getOrCreate()

#1. 关联表
df_travel = spark.table("dwd.dwd_user_travel_sequence_info")
df_poi = spark.table("dwd.dwd_poi_info").select("poi_id", "city", "category", "region")
df_joined = df_travel.join(
    F.broadcast(df_poi), on=["poi_id"], how="left")   #left保护用户行为不丢失

#2. 提取季度
df_joined = df_joined.withColumn(
    "travel_quarter",
    F.ceil(F.col("travel_month") / 3)
)

df_joined = df_joined.cache()

#3. 开始做基础分组

#从 df_joined 中过滤掉 category 为"未分类"的行
df_joined_with_category = df_joined.filter(F.col("category") != "未分类").cache()
#与category无关的基础聚合
df_agg_base = df_joined.groupby(
    "blog_id", "travel_year", "travel_quarter"
).agg(
    #总量指标
    F.countDistinct("trip_id").alias("trip_total_cnt"),    #本季度旅行次数
    F.count("*").alias("total_spot_cnt"),    #本季度游览的景点总数
    F.countDistinct("city").alias("unique_city_cnt"),    #本季度去过的城市总数（去重）

    #分布指标
    F.collect_set("region").alias("region_arr"),    #本季度去过的区域集合
    F.collect_set("travel_partners").alias("partner_arr"),    #本季度的游玩同伴类型集合

    #时间特征
    F.min("travel_date").alias("first_trip_date"),
    F.max("travel_date").alias("last_trip_date")
)

#与category相关的基础聚合
df_agg_cate = df_joined_with_category.groupby(
    "blog_id", "travel_year", "travel_quarter"
).agg(
    F.countDistinct("category").alias("unique_category_cnt"),    #本季度去过的景点类型总数（去重）
    F.collect_set("category").alias("category_arr"),  # 本季度去过的的景点类型集合
)

#合并两个表
df_agg = df_agg_base.join(
    df_agg_cate,
    on=["blog_id", "travel_year", "travel_quarter"],
    how="left"
)

#存在有人某季度只去了未分类的景点的情况
df_agg = df_agg.fillna(0, subset=["unique_category_cnt"])

#4. 计算跨区域次数
df_trip_region_tmp = df_joined.groupby(
    "blog_id", "trip_id", "travel_year", "travel_quarter"
).agg(
    F.countDistinct("region").alias("region_cnt")
)

df_cross = df_trip_region_tmp.filter(F.col("region_cnt") >= 2).groupby(
    "blog_id", "travel_year", "travel_quarter"
).agg(
    F.count("*").alias("cross_region_trip_cnt")
)

#5. 偏好指标
    #--城市维度--
df_city_cnt_tmp = df_joined.groupby(
    "blog_id", "travel_year", "travel_quarter", "city"
).agg(
    F.countDistinct("trip_id").alias("city_visit_cnt")
)

#本季度去的城市以及次数
df_city_map = df_city_cnt_tmp.groupby(
    "blog_id", "travel_year", "travel_quarter"
).agg(
    F.map_from_entries(
        F.collect_list(
            F.struct("city", "city_visit_cnt")
        )
    ).alias("city_visit_map")
)

# 从city_visit_map取value最大的所有key（可能有并列）
df_top_cities = df_city_map.select(
    "blog_id", "travel_year", "travel_quarter",
    F.expr(
        "transform("
        "   filter(map_entries(city_visit_map), x -> x.value = array_max(map_values(city_visit_map))),"
        "   x -> x.key"
        ")"
    ).alias("top_city")
)

    #--景点分类维度--
df_cate_cnt_tmp = df_joined_with_category.groupby(
    "blog_id", "travel_year", "travel_quarter", "category"
).agg(
    F.count("*").alias("cate_visit_cnt")
)

#本季度去的景点分类以及次数
df_cate_map = df_cate_cnt_tmp.groupby(
    "blog_id", "travel_year", "travel_quarter"
).agg(
    F.map_from_entries(
        F.collect_list(
            F.struct("category", "cate_visit_cnt")
        )
    ).alias("cate_visit_map")
)

# 从cate_visit_map取value最大的所有key（可能有并列）
df_top_cate = df_cate_map.select(
    "blog_id", "travel_year", "travel_quarter",
    F.expr(
        "transform("
        "   filter(map_entries(cate_visit_map), x -> x.value = array_max(map_values(cate_visit_map))),"
        "   x -> x.key"
        ")"
    ).alias("top_category")
)

    #--独自出行次数--
df_trip_partner_tmp = df_joined.select(
    "blog_id", "trip_id", "travel_year", "travel_quarter", "travel_partners"
).dropDuplicates(["blog_id", "trip_id"])

df_solo = df_trip_partner_tmp.filter(
    F.col("travel_partners") == "Solo"
).groupby(
    "blog_id", "travel_year", "travel_quarter"
).agg(
    F.count("*").alias("solo_trip_cnt"),
)

#6. 计算本季度出行最多的月份
df_month_cnt_tmp = df_joined.groupby(
    "blog_id", "travel_year", "travel_quarter", "travel_month"
).agg(
    F.count("*").alias("month_visit_cnt")
)

window_spec_month_tmp = Window.partitionBy("blog_id", "travel_year", "travel_quarter").\
    orderBy(F.col("month_visit_cnt").desc())

df_top_month = df_month_cnt_tmp.withColumn(
    "rn", F.row_number().over(window_spec_month_tmp)
).filter(F.col("rn") == 1).select(
    "blog_id", "travel_year", "travel_quarter",
    F.col("travel_month").alias("peak_month")
)

#7. 聚合
df_result = df_agg
df_result = df_result.join(df_cross, on=["blog_id", "travel_year", "travel_quarter"], how="left")
df_result = df_result.fillna(value=0, subset=["cross_region_trip_cnt"])

df_result = df_result.join(df_city_map, on=["blog_id", "travel_year", "travel_quarter"], how="left")
df_result = df_result.join(df_cate_map, on=["blog_id", "travel_year", "travel_quarter"], how="left")
df_result = df_result.join(df_solo, on=["blog_id", "travel_year", "travel_quarter"], how="left")
df_result = df_result.fillna(value=0, subset=["solo_trip_cnt"])
df_result = df_result.join(df_top_month, on=["blog_id", "travel_year", "travel_quarter"], how="left")
df_result = df_result.join(df_top_cities, on=["blog_id", "travel_year", "travel_quarter"], how="left")
df_result = df_result.join(df_top_cate, on=["blog_id", "travel_year", "travel_quarter"], how="left")

df_result = df_result.withColumn("travel_level", F.when(F.col("trip_total_cnt") >= 4, "高频").\
                        when((F.col("trip_total_cnt") == 2) | (F.col("trip_total_cnt") == 3), "中频").\
                        otherwise("低频"))
df_result = df_result.withColumn("region_type",
    F.when(F.size(F.col("region_arr")) >= 2, "全国型").\
    when((F.size(F.col("region_arr")) == 1) & (F.size(F.map_keys(F.col("city_visit_map"))) >= 2), "区域型").\
    when(F.size(F.map_keys(F.col("city_visit_map"))) == 1, "单城型"))

df_result = df_result.withColumn("avg_spot_per_trip",
    F.when(F.col("trip_total_cnt") > 0, F.round(F.col("total_spot_cnt") / F.col("trip_total_cnt"), 4)).otherwise(0))

df_result = df_result.withColumn("avg_city_per_trip",
    F.when(F.col("trip_total_cnt") > 0, F.round(F.col("unique_city_cnt") / F.col("trip_total_cnt"), 4)).otherwise(0))

df_result = df_result.withColumn("avg_trip_per_month",
    F.when(F.col("trip_total_cnt") > 0, F.round(F.col("trip_total_cnt") / 3, 4)).otherwise(0))

df_result = df_result.withColumn("solo_trip_ratio",
        F.when(F.col("trip_total_cnt") > 0, F.round(F.col("solo_trip_cnt") / F.col("trip_total_cnt"), 4)).otherwise(0))

#8. 插入数据
df_final = df_result.select(
    "blog_id", "travel_level", "region_type",
    "trip_total_cnt", "total_spot_cnt", "unique_city_cnt",
    "unique_category_cnt", "cross_region_trip_cnt", "avg_spot_per_trip",
    "avg_city_per_trip", "avg_trip_per_month",
    "region_arr", "partner_arr",
    "top_city", "top_category", "solo_trip_ratio",
    "city_visit_map", "cate_visit_map",
    "peak_month", "first_trip_date", "last_trip_date",
    "travel_year", "travel_quarter"
)

#9. 写入表
df_final.write.mode("overwrite").\
    partitionBy(
    "travel_year", "travel_quarter"
).saveAsTable("dws.dws_user_travel_quarter")

df_joined.unpersist()
spark.stop()
