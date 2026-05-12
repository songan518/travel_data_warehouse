--dwd层-景区清洗明细表
create external table if not exists dwd.dwd_poi_info(
    poi_id      string comment "景区id",
    name        string comment "景区名称",
    city        string comment "所在城市",
    latitude    double comment "纬度",
    longitude   double comment "经度",
    label       array<string> comment "标签",
    category    string comment "景点分类",
    region      string comment "区域"
)comment "dwd层-景区清洗明细表"
stored as parquet;

--dwd层-用户出行明细清洗表
create external table if not exists dwd.dwd_user_travel_sequence_info(
    blog_id             string comment "匿名用户id",
    trip_id             string comment "旅程id",
    travel_date         date comment "出行日期",
    travel_partners     string comment "同行伙伴类型",
    poi_id              string comment "景点id"
)comment "dwd层-用户出行明细清洗表"
partitioned by(travel_year int, travel_month int)
stored as parquet;
