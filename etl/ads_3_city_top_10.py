"""
ads层-城市热度排行榜
功能：
 1. 读取dws层城市热度宽表
 2. 按季度计算城市热度排名
 3. 取每季度前10名
 4. 写入ads表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("ads_city_top_10").\
        master("local[*]").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://your_server:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        config("spark.sql.catalogImplementation", "hive"). \
        getOrCreate()

#1. 导入数据
df_city = spark.table("dws.dws_city_travel_quarter")

#2. 计算季度热度排名
df_city = df_city.withColumn("rank",
    F.row_number().over(Window.partitionBy("travel_year", "travel_quarter").orderBy(F.col("visit_trip_cnt").desc()))
).filter(F.col("rank") <= 10)

#3. 汇总
df_city = df_city.withColumn("year_quarter",
    F.concat("travel_year", F.lit("Q"), "travel_quarter"))

df_final = df_city.select(
    "year_quarter","city",
    "region", "visit_user_cnt",
    "visit_trip_cnt", "unique_attraction_cnt",
    "avg_spot_per_user", "rank", "is_hot_rising"
).orderBy("year_quarter", "rank")

df_final.write.mode("overwrite").saveAsTable("ads.ads_city_top10")

spark.stop()
