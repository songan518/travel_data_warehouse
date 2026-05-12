--dws层-用户季度出行主题宽表
create external table if not exists dws.dws_user_travel_quarter(
    --维度
    blog_id                 string comment "匿名用户id",

    --用户分层标签
    travel_level            string comment "出行频次分层：高频(>=4次)/中频(2-3次)/低频(0-1次)",
    region_type             string comment "活动范围类型：全国型(跨2个及以上区域)/区域型(1个区域内多城市)/单城型(只去一个城市)",

    --总量指标
    trip_total_cnt          int comment "本季度总出行次数",
    total_spot_cnt          int comment "本季度累计游玩景点数",
    unique_city_cnt         int comment "本季度到访不重复城市数",
    unique_category_cnt     int comment "本季度游玩不重复景点分类数",
    cross_region_trip_cnt   int comment "跨区域出行次数(一次去了两个及以上区域的旅程数)",

    --平均值指标
    avg_spot_per_trip       double comment "平均每次出行游玩景点数",
    avg_city_per_trip       double comment "平均每次出行到访城市数",
    avg_trip_per_month      double comment "月均出行次数",

    --分布于偏好指标
    region_arr              array<string> comment "本季度到访区域列表(去重)",
    partner_arr             array<string> comment "本季度处有同伴类型列表(去重)",
    top_city                array<string> comment "本季度去过最多次的城市",
    top_category            array<string> comment "本季度玩的最多的景点分类",
    solo_trip_ratio         double comment "独自出行次数占比",
    city_visit_map          map<string, int> comment "本季度每个城市的访问次数",
    category_visit_map      map<string, int> comment "本季度每个景点分类的访问次数",

    --时间特征
    peak_month              int comment "本季度出行最多的月份",
    first_trip_date         string comment "本季度首次出行日期",
    last_trip_date          string comment "本季度末次出行日期"
)comment "dws层-用户季度出行主题宽表"
partitioned by (travel_year int, travel_quarter int)
stored as parquet ;

--dws层-景点热度主题表
create external table if not exists dws.dws_attraction_travel_quarter(
    --维度
    poi_id string comment "景点ID",
    attraction_name string comment "景点名称",
    city string comment "所在城市",
    category string comment "景点分类",
    region string comment "所在区域",

    --热度指标
    visit_user_cnt int comment "本季度到访该景点的独立用户数",
    visit_trip_cnt int comment "本季度该景点被游玩的总次数",
    visit_total_cnt int comment "本季度该景点被游玩的总人次",

    --排名指标
    user_rank_in_city int comment "本城市内热度排名",
    user_rank_in_region int comment "本区域内热度排名",
    user_rank_overall int comment "全量热度排名",

    --游客画像指标
    solo_user_ratio double comment "独自出行用户占比",

    --环比变化指标
    user_cnt_growth_rate double comment "对比上季度用户数增长率",
    is_hot_rising string comment "是否热度上升：是/否/无上期数据"
)comment "dws层-景点热度主题表"
partitioned by (travel_year int, travel_quarter int)
stored as parquet;

--dws层-城市季度热度主题表
create external table if not exists dws.dws_city_travel_quarter(
    --维度
    city string comment "城市名称",
    region string comment "区域名称",

    --热度指标
    visit_user_cnt int comment "本季度到访该城市的独立用户数",
    visit_trip_cnt int comment "本季度该城市被访问的总次数",
    visit_spot_cnt int comment "本季度该城市被游玩的总景点数",
    unique_attraction_cnt int comment "本季度该城市被游玩的不重复景点数",

    --游客特征
    avg_spot_per_user double comment "用户在该城市平均游玩景点数",
    solo_user_ratio double comment "独自出行用户占比",

    --排名指标
    user_rank_in_region int comment "本区域内热度排名",
    user_rank_overall int comment "全量热度排名",

    --环比变化指标
    user_cnt_growth_rate double comment "对比上季度用户数增长率",
    is_hot_rising string comment "是否热度上升：是/否/无上期数据"
)comment "dws层-城市季度热度主题表"
partitioned by (travel_year int, travel_quarter int)
stored as parquet;
