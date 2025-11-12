#1
CREATE TABLE IF NOT EXISTS user_events 
(
	event_time DateTime,
	user_id UInt32,
	ad_id UInt32,
	platform String,
	price Float
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 Days;

#2.1
CREATE TABLE IF NOT EXISTS goods_src
(
	category_id UInt32,
	category_name String,
	parent_id UInt32
)
ENGINE = MergeTree()
ORDER BY category_id;

CREATE DICTIONARY goods_dict
(
	category_id UInt32,
	category_name String,
	parent_id UInt32
)
PRIMARY KEY category_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'default' TABLE 'goods_src'))
LAYOUT(HASHED());


#2.2
CREATE TABLE IF NOT EXISTS goods_cat
(
    category_id   UInt32,
    category_name String,
    parent_id     UInt32
)
ENGINE = TinyLog;


#3
CREATE TABLE IF NOT EXISTS orders
(
	order_id UUID,
	user_id UInt64,
	order_date DateTime,
	amount Decimal(10,2),
	status Enum('created' = 1, 'paid' = 2, 'canceled' = 3)
)
ENGINE = ReplacingMergeTree(order_date)
ORDER BY order_id;


#4
CREATE TABLE IF NOT EXISTS sessions
(
    session_id  UUID,
    user_id     UInt64,
    start_time  DateTime,
    duration_sec UInt32,
    pages       Array(String)
)
ENGINE = MergeTree
ORDER BY (user_id, start_time);


#5
CREATE TABLE IF NOT EXISTS daily_stat 
(
	event_day Date,
	ad_id UInt64,
	clicks AggregateFunction(count, UInt64),
	unique_user AggregateFunction(uniq, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (event_day, ad_id);


#6
CREATE TABLE IF NOT EXISTS session_dur
(
	user_id UInt64,
	avg_duration AggregateFunction(avg, Uint64),
	max_duration AggregateFunction(max, Uint64)
)
ENGINE = AggregatingMergeTree()
ORDER BY user_id;