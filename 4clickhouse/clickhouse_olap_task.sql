--raw events table
CREATE TABLE user_events
(
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;


--aggregated stats table
CREATE TABLE daily_stat
(
	event_date Date,
	event_type String,
	unique_users AggregateFunction(uniq, UInt32),
	total_spent AggregateFunction(sum, UInt32),
	total_actions AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;


--mv
CREATE MATERIALIZED VIEW mv_stat
TO daily_stat AS
	SELECT toDate(event_time) AS event_date,
	event_type,
	uniqState(user_id) AS unique_users,
	sumState(points_spent) AS total_spent,
	countState() AS total_actions
		FROM user_events
		GROUP BY event_date, event_type;


--test data
INSERT INTO user_events (user_id, event_type, points_spent, event_time) VALUES
(1, 'login',    0, now() - toIntervalDay(10)),
(2, 'signup',   0, now() - toIntervalDay(10)),
(3, 'login',    0, now() - toIntervalDay(10)),
(1, 'login',    0, now() - toIntervalDay(7)),
(2, 'login',    0, now() - toIntervalDay(7)),
(3, 'purchase', 30, now() - toIntervalDay(7)),
(1, 'purchase', 50, now() - toIntervalDay(5)),
(2, 'logout',   0, now() - toIntervalDay(5)),
(4, 'login',    0, now() - toIntervalDay(5)),
(1, 'login',    0, now() - toIntervalDay(3)),
(3, 'purchase', 70, now() - toIntervalDay(3)),
(5, 'signup',   0, now() - toIntervalDay(3)),
(2, 'purchase', 20, now() - toIntervalDay(1)),
(4, 'logout',   0, now() - toIntervalDay(1)),
(5, 'login',    0, now() - toIntervalDay(1)),
(1, 'purchase', 25, now()),
(2, 'login',    0, now()),
(3, 'logout',   0, now()),
(6, 'signup',   0, now()),
(6, 'purchase', 100, now());


--retention rate query
WITH 
first_event AS (
	SELECT
		user_id,
		MIN(event_time) AS first_event
	FROM user_events
	GROUP BY user_id
),
user_retention AS (
	SELECT
		ue.user_id,
		f.first_event,
		countIf(
			ue.event_time > f.first_event
			AND ue.event_time <= f.first_event + toIntervalDay(7)
		) > 0 AS has_return_in_7d
	FROM user_events ue
	JOIN first_event f USING (user_id)
	GROUP BY ue.user_id, f.first_event
)
SELECT 
	COUNT() AS total_users_day_0,
	countIf(has_return_in_7d) AS returned_in_7_days,
	ROUND(
		100.0 * countIf(has_return_in_7d) / COUNT(), 
		2
	) AS retention_7d_percent
FROM user_retention;


--daily analytics via merge
SELECT
	event_date,
	event_type,
	uniqMerge(unique_users) AS unique_users,
	sumMerge(total_spent) AS total_spent,
	countMerge(total_actions) AS total_actions
FROM daily_stat
GROUP BY event_date, event_type
ORDER BY event_date, event_type;