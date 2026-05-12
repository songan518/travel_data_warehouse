# Travel Data Warehouse - 旅游用户行为数仓项目

基于 Hive + Spark 搭建的四层离线数仓，对旅游用户出行数据进行分层存储与分析，产出用户画像、景点热度排行、城市热度排行等业务指标。

## 技术栈

- Hadoop (HDFS) / Hive / Spark (PySpark)
- Parquet 列式存储
- DataGrip / PyCharm

## 核心功能点

- **星型模型设计**：DWD层以用户出行明细为事实表，景区信息为维度表，支撑多维度分析
- **用户分层**：按出行频次划分高频/中频/低频用户，按活动范围划分全国型/区域型/单城型
- **环比热度分析**：计算景点和城市的季度热度环比增长率，识别热度上升趋势
- **复杂数据类型**：使用 array 存储多值标签，map 存储城市访问次数分布
- **ETL优化**：使用 broadcast join 优化小表关联，cache 减少重复计算

## 数据说明

- 数据来源：Chinese Tourism User Behavior Dataset（Figshare公开数据集）
- 数据范围：原始数据经数据清洗后保留2011年 - 2021年的有效记录 
- 核心表：用户出行记录约7万条，景区信息约2万个

## 数仓分层

| 层级 | 说明 |
|------|------|
| ODS | 原始数据层 — 用户出行轨迹、景区维度原始数据 |
| DWD | 明细数据层 — 数据清洗、标签解析、地理区域划分 |
| DWS | 服务数据层 — 按用户/景点/城市三个主题轻度聚合 |
| ADS | 应用数据层 — 用户画像、Top50景点、城市Top10 |

## 项目结构

```
travel_data_warehouse/
├── sql/
│   ├── create_databases.sql
│   ├── ods_tables.sql
│   ├── dwd_tables.sql
│   ├── dws_tables.sql
│   └── ads_tables.sql
├── etl/
│   ├── ods_load_data.py
│   ├── dwd_poi_info.py
│   ├── dwd_user_travel.py
│   ├── dws_user_travel_quarter.py
│   ├── dws_attraction_quarter.py
│   ├── dws_city_quarter.py
│   ├── ads_user_portrait.py
│   ├── ads_top50_attractions.py
│   └── ads_city_top10.py
└── README.md
```

## 运行方式

1. 按 ods → dwd → dws → ads 顺序执行 `sql/` 下的建表语句
2. 按相同顺序执行 `etl/` 下的 PySpark 脚本
3. 脚本中的 `your_server` 请替换为实际 Hadoop 集群地址

## 待优化

- 接入调度工具（如 Azkaban / DolphinScheduler）实现自动化
- 引入城市-省份-区域映射表，优化区域划分精度
