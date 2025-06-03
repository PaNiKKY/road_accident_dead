-- Creates a time dimension table with a record for every minute of the day.
CREATE OR REPLACE TABLE accident_data.dim_time AS

WITH minute_sequence AS (
  -- Generate a sequence of numbers from 0 to 1439 (the number of minutes in a day)
  SELECT
    minute_of_day
  FROM
    UNNEST(GENERATE_ARRAY(0, 24 * 60 - 1)) AS minute_of_day
),

time_parts AS (
  -- Convert the minute offset into a TIME data type
  SELECT
    TIME_ADD(TIME(0, 0, 0), INTERVAL minute_of_day MINUTE) AS full_time,
    minute_of_day
  FROM
    minute_sequence
)

-- Extract all the different time components and attributes
SELECT
  CAST(FORMAT_TIME('%H%M', full_time) AS INT64) AS time_key,
  full_time,
  FORMAT_TIME('%R', full_time) AS time_string_24h,     -- e.g., 14:30
  FORMAT_TIME('%I:%M %p', full_time) AS time_string_12h, -- e.g., 02:30 PM
  EXTRACT(HOUR FROM full_time) AS hour_24,
  CAST(FORMAT_TIME('%I', full_time) AS INT64) AS hour_12,
  EXTRACT(MINUTE FROM full_time) AS minute_of_hour,
  minute_of_day,
  FORMAT_TIME('%p', full_time) AS am_pm,
  CASE
    WHEN EXTRACT(HOUR FROM full_time) BETWEEN 6 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM full_time) BETWEEN 12 AND 16 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM full_time) BETWEEN 17 AND 20 THEN 'Evening'
    ELSE 'Night'
  END AS time_of_day_period
FROM
  time_parts
ORDER BY
  time_key;