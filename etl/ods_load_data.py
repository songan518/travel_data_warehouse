"""
ODS层数据加载脚本
功能：从HDFS加载CSV数据到ODS层
"""

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("ods_load_from_hdfs").\
        master("local[*]"). \
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://your_server:9083").\
        config("spark.sql.warehouse.dir", "/user/hive/warehouse").\
        config("spark.sql.catalogImplementation", "hive").\
        getOrCreate()

    poi_path = "hdfs://your_server/travel/ods/source/POIs_V2.csv"
    user_path = "hdfs://your_server/travel/ods/source/Visit_Sequences_V2.csv"

#1. POI维度表
    spark.read.csv(poi_path, header=True, encoding="utf-8").\
        selectExpr(
        "Encrypted_ID AS poi_id",
        "Name_ZH AS name_zh",
        "Name_EN AS name_en",
        "City_ZH AS city_zh",
        "City_EN AS city_en",
        "Latitude_GCJ02 AS latitude",
        "Longitude_GCJ02 AS longitude",
        "Label_ZH AS label_zh",
        "Label_EN AS label_en"
    ).\
        write.mode("overwrite").\
        saveAsTable("ods.ods_poi_dim")

#2. 用户轨迹表
    spark.read.csv(user_path, header=True, encoding="utf-8"). \
        selectExpr(
        "Anonymized_Blog_ID AS blog_id",
        "Retrieval_Date AS retrieval_date",
        "Departure_Date AS departure_date",
        "Travel_Partners AS travel_partners",
        "Visit_Sequence AS visit_sequence"
    ).\
        write.mode("overwrite"). \
        saveAsTable("ods.ods_user_travel_sequence")

    spark.stop()
