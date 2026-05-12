"""
dwd层景区维度清洗脚本
功能：
 1. 读取ods层数据
 2. 过滤空值
 3. 类型转换
 4. 过滤非国内经纬度范围
 5. 填充空标签
 6. 标签拆分为数组
 7. 地理区域划分
 8. 景点分类打标
 9. 写入dwd表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("dwd_poi_info").\
        master("local[*]").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://your_server:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        config("spark.sql.catalogImplementation", "hive"). \
        getOrCreate()

#1. 读取ods层数据,初步筛掉与分析无关的各种英文名称
df = spark.table("ods.ods_poi_dim").\
    select(
    "poi_id",
    "name_zh",
    "city_zh",
    "latitude",
    "longitude",
    "label_zh"
)

#2. 过滤空值
df = df.dropna(
    subset=["poi_id", "name_zh", "city_zh", "latitude", "longitude"],
    how="any"
)

#3. 修改数据格式
df = df.withColumn("latitude", F.col("latitude").cast("double"))
df = df.withColumn("longitude", F.col("longitude").cast("double"))

#4. 过滤非国内旅游地
df = df.filter(
    ((F.col("latitude") >= 4) &
    (F.col("latitude") <= 54)) &
    ((F.col("longitude") >= 73) & (F.col("longitude") <= 135))
)

#5.查看经纬度范围判断是否合理
df.selectExpr(
"min(latitude) min_lat",
        "max(latitude) max_lat",
        "min(longitude) min_lon",
        "max(longitude) max_lon"
).show()

#6. 填空标签
df = df.fillna(
    value = "无标签", subset=["label_zh"]
)

df.groupBy("label_zh").count().orderBy(F.col("count").desc()).show(50, truncate=False)

#7. 标签转为数组形式，切割分号
df = df.withColumn(
    "label", F.split(F.col("label_zh"), ";")
)

#8. 划分地理区域
df = df.withColumn("region",
    F.when((F.col("latitude") >= 40) & (F.col("longitude") >= 120), "东北")
    .when((F.col("latitude") >= 35) & (F.col("latitude") < 40) &
     (F.col("longitude") >= 110) & (F.col("longitude") < 120), "华北")
    .when((F.col("latitude") >= 35) & (F.col("longitude") < 110), "西北")
    .when((F.col("latitude") >= 28) & (F.col("latitude") < 35) &
     (F.col("longitude") >= 115), "华东")
    .when((F.col("latitude") >= 28) & (F.col("latitude") < 35) &
     (F.col("longitude") >= 110) & (F.col("longitude") < 115), "华中")
    .when((F.col("latitude") <= 28) & (F.col("longitude") >= 110), "华南")
    .otherwise("西南")
)
# 在划分区域之后，检查是否有null值
df.filter(F.col("region").isNull()).select("poi_id", "name_zh", "latitude", "longitude").show(50)

#9. 浅层次划分标签
df = df.withColumn("category",
    F.when(F.col("label_zh").rlike("乐园|游乐场|漂流|拓展|骑行|露营|欢乐谷|方特|主题体验"), "主题游乐")
    .when(F.col("label_zh").rlike("博物馆|美术馆|纪念馆|展览馆|科技馆|影视基地|故居|名人故居"), "文化场馆")
    .when(F.col("label_zh").rlike("古镇|古城|古建|古建筑|寺庙|石窟|园林|古村落|城墙|文物古迹|历史遗址|古战场|老城|文庙|道观"), "历史人文古迹")
    .when(F.col("label_zh").rlike("城市地标|地标建筑|城市标志建筑|高楼|摩天轮|网红打卡点"), "都市地标")
    .when(F.col("label_zh").rlike("公园|商圈|步行街|广场|绿地|城市公园"), "都市休闲")
    .when(F.col("label_zh").rlike("乡村|农庄|农家乐|采摘|田园|民俗村"), "乡村生态")
    .when(F.col("label_zh").rlike("山|河|江|湖|海|瀑布|草原|峡谷|海滩|海岛|红树林|温泉|生态|山水|风景|自然景观|湖泊|江河|溪流|湿地|森林|花海|赏花"), "自然风光")
    .when(F.col("label_zh") == "无标签", "未分类")
    .otherwise("其他景点")
)
# 区域划分说明：
# 当前基于经纬度范围做粗略划分，边界城市可能归入错误的区域。
# 例如：陕西安康（纬度约32.5°，经度约109°）因纬度<35且经度<110，
# 会被归入"西南"，但实际上属于西北地区（陕西省）。
# 后续可引入城市-省份-区域映射表来精确划分。

# 在ETL最后打印一下每个分类的数量，看看分布是否合理
df.groupBy("category").count().show()
df.groupBy("region").count().show()

#10. 匹配建表字段准备写入
df_final = df.select(
    "poi_id",
    F.col("name_zh").alias("name"),
    F.col("city_zh").alias("city"),
    "latitude",
    "longitude",
    "label",
    "category",
    "region"
)

df_final.write.mode("overwrite").saveAsTable("dwd.dwd_poi_info")

spark.stop()
