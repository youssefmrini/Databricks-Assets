-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Warehouse Optimization

-- COMMAND ----------

WITH warehouse_snapshot AS (
  SELECT
    *
  FROM
    main.dbsql_warehouse_advisor.warehouse_current w
  WHERE
    (
      lower(w.warehouse_id) = lower(:warehouse_id)
    )
  LIMIT
    1
), query_queuing_history AS (
  SELECT
    statement_id,
    warehouse_id AS warehouse_id,
    QueryRuntimeSeconds * 1000 AS total_duration_ms,
    COALESCE(QueueQueryTime * 1000, 0) AS queueing_ms,
    -- Queue time is the total of waiting_at_capacity and waiting_for_compute in the MV
    ExecutionQueryTime * 1000 AS execution_duration_ms,
    start_time AS query_start_timestamp,
    TIMESTAMPADD(MILLISECOND, queueing_ms, query_start_timestamp) AS queuing_end_timestamp,
    CASE
      WHEN queueing_ms > 0 THEN 1
      ELSE 0
    END AS IsQueued,
    end_time AS query_end_timestamp,
    DATE_TRUNC('SECOND', query_start_timestamp) AS query_start_second,
    DATE_TRUNC('SECOND', queuing_end_timestamp) AS query_queue_end_second
  FROM
    main.dbsql_warehouse_advisor.warehouse_query_history qh
  WHERE
    qh.start_time BETWEEN CAST(:start_time AS TIMESTAMP)
    AND CAST(:end_time AS TIMESTAMP)
    AND qh.warehouse_id IN (
      SELECT
        DISTINCT warehouse_id
      FROM
        warehouse_snapshot
    )
),
full_series AS (
  SELECT
    explode(
      sequence(
        (
          SELECT
            date_trunc('SECOND', MIN(query_start_timestamp))
          FROM
            query_queuing_history
        ),
        (
          SELECT
            date_trunc('SECOND', MAX(query_end_timestamp))
          FROM
            query_queuing_history
        ),
        INTERVAL 1 SECOND
      )
    ) AS secondChunk
),
queuing_details AS (
  SELECT
    spine.secondChunk AS second_chunk,
    -- IsQueued during window flag
    CASE
      WHEN (
        spine.secondChunk >= DATE_TRUNC('SECOND', q.query_start_timestamp)
        AND spine.secondChunk <= q.queuing_end_timestamp
      )
      AND q.IsQueued = 1 THEN 1
      ELSE 0
    END AS IsQueuedDuringSecond,
    -- Peak running
    CASE
      WHEN (
        spine.secondChunk >= DATE_TRUNC('SECOND', q.queuing_end_timestamp)
        AND spine.secondChunk <= q.query_end_timestamp
      ) THEN 1
      ELSE 0
    END AS IsRunningDuringSecond,
    q.query_start_timestamp,
    q.queuing_end_timestamp,
    q.query_end_timestamp,
    q.statement_id
  FROM
    full_series AS spine
    LEFT JOIN query_queuing_history AS q ON (
      spine.secondChunk >= DATE_TRUNC('SECOND', q.query_start_timestamp)
      AND spine.secondChunk <= q.query_end_timestamp
    )
  WHERE
    q.statement_id IS NOT NULL
),
second_level_queuing AS (
  SELECT
    DATE_TRUNC('SECOND', second_chunk) AS second_int,
    SUM(IsQueuedDuringSecond) AS QueuedCount,
    SUM(IsRunningDuringSecond) AS RunningCount
  FROM
    queuing_details
  GROUP BY
    DATE_TRUNC('SECOND', second_chunk)
  ORDER BY
    second_int
)
SELECT
  DATE_ADD(
    MINUTE,
    FLOOR(
      EXTRACT(
        MINUTE
        FROM
          second_int
      ) / 5
    ) * 5 - EXTRACT(
      MINUTE
      FROM
        second_int
    ),
    DATE_TRUNC('MINUTE', second_int)
  ) AS five_min_interval,
  MAX(RunningCount) AS PeakRunning,
  MAX(QueuedCount) AS PeakQueued
FROM
  second_level_queuing
GROUP BY
  five_min_interval
ORDER BY
  five_min_interval