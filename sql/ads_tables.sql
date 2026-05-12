--ads层-用户画像表
create external table if not exists ads.ads_user_portrait(
    blog_id            string comment "用户id",
    total_trips        int comment "总出行次数",
    total_spots        int comment "总游览景点数",
    total_cities       int comment "累计到访不同城市数",
    total_regions      int comment "累计到访不同区域数",
    active_years       int comment "活跃年份数",
    first_active_year  int comment "首次出行年份",
    last_active_year   int comment "最后出行年份",
    avg_spots_per_trip double comment "平均每次游览景点数",
    avg_solo_ratio     double comment "平均独自出行比例",
    favorite_city      array<string> comment "最常去的城市",
    favorite_category  array<string> comment "最偏好的景点类型",
    travel_level       string comment "出行活跃等级",
    region_type        string comment "出行范围类型"
)comment "ads层-用户画像表"
stored as parquet;

--ads层-热门景点排行榜
create external table if not exists ads.ads_top50_attractions(
    year_quarter         string comment "年份季度",
    poi_id               string comment "景点id",
    name                 string comment "景点名称",
    city                 string comment "所在城市",
    category             string comment "景点分类",
    region               string comment "所在区域",
    visit_user_cnt       int comment "访问用户数",
    visit_trip_cnt       int comment  "被游玩次数",
    rank                 int comment "季度热度排名",
    is_hot_rising        string comment "是否热度上升",
    user_cnt_growth_rate double comment "环比增长率"
)comment "ads层热门景点排行榜"
stored as parquet ;

--ads层-城市热度top10
create external table if not exists ads.ads_city_top10(
    year_quarter string comment "年份季度",
    city string comment "城市名称",
    region string comment "所属区域",
    visit_user_cnt int comment "访问用户数",
    visit_trip_cnt int comment "访问次数",
    unique_attraction_cnt int comment "被访问的不同景点数",
    avg_spot_per_user double comment "人均游览总数",
    rank int comment "季度热度排名",
    is_hot_rising string comment ""
)comment "ads层-城市热度top10"
stored as parquet ;
