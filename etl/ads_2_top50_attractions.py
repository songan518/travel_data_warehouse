"""
ads层-热门景点排行榜
功能：
 1. 读取dws层景点热度宽表
 2. 按季度计算景点热度排名
 3. 取每季度前50名
 4. 写入ads表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("ads_top50_attractions").\
        master("local[*]").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://your_server:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        config("spark.sql.catalogImplementation", "hive"). \
        getOrCreate()

#1. 导入数据
df_attraction = spark.table("dws.dws_attraction_travel_quarter")

#2. 根据该季度景区的被游览次数排名 取前五十
df_attraction = df_attraction.withColumn("rank",
    F.row_number().over(Window.partitionBy(F.col("travel_year"), F.col("travel_quarter")).orderBy(F.col("visit_trip_cnt").desc()))
).\
    filter(F.col("rank") <= 50)

#3. 设置新列
df_attraction = df_attraction.withColumn("year_quarter",
    F.concat("travel_year", F.lit("Q"), "travel_quarter")
)

#4. 汇总
df_final = df_attraction.select(
    "year_quarter","poi_id","name",
    "city", "category", "region",
    "visit_user_cnt", "visit_trip_cnt",
    "rank", "is_hot_rising", "user_cnt_growth_rate"
).orderBy("year_quarter", "rank")

df_final.write.mode("overwrite").saveAsTable("ads.ads_top50_attractions")

spark.stop()
