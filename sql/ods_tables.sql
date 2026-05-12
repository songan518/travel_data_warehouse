--ods层-用户出行轨迹原始表
create external table if not exists ods.ods_user_travel_sequence(
    blog_id         string comment "匿名用户id",
    retrieval_date  string comment "数据抓取日期",
    departure_date  string comment "出行日期",
    travel_partners string comment "同行伙伴类型",
    visit_sequence  string comment "景区序列"
)comment "ods层-用户出行轨迹原始表"
row format delimited fields terminated by ","
stored as textfile ;

--ods层-景区维度表
create external table if not exists ods.ods_poi_dim(
    poi_id    string comment "景区id",
    name_zh   string comment "景区名称（中文）",
    name_en   string comment "景区名称（英文）",
    city_zh   string comment "所在城市（中文）",
    city_en   string comment "所在城市（英文）",
    latitude  decimal(10,6) comment "纬度",
    longitude decimal(10,6) comment "经度",
    label_zh  string comment "景区标签（中文）",
    label_en  string comment "景区标签（英文）"
)comment "ods层-景区维度表"
row format delimited fields terminated by ","
stored as textfile ;
