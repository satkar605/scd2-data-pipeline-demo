-- ===================================================================================
-- Task 4: SCD Type 2 Table Backfill for `actors_history_scd`
-- ===================================================================================
-- This script performs a full historical backfill for actors' history, creating a 
-- complete Type 2 Slowly Changing Dimension table from the raw `actor_films` data.
--
-- Logic:
-- 1. Generate Yearly Snapshots: For each year from 1970 to 2021, calculate each
--    actor's `quality_class` and `is_active` status based on their films from
--    that specific year. This creates a historical record of what was true for
--    each actor at the end of each year.
--
-- 2. Detect Changes (Create Streaks): Compare each yearly snapshot to the previous
--    year's snapshot for the same actor. By flagging changes, we can assign a
--    `streak_identifier` to group consecutive years where an actor's status
--    remained the same.
--
-- 3. Consolidate Streaks into SCD2 Rows: Group the yearly snapshots by the
--    `streak_identifier` to consolidate them into final SCD2 rows. This creates
--    the `start_year` and `end_year` for each version of an actor's history.
--
-- 4. Load into Final Table: Insert the generated SCD2 rows into the
--    `actors_history_scd` table. The `current_year` is hardcoded to 2021 to
--    signify that this backfill contains all data up to that point.
-- ===================================================================================

-- Truncate the table to ensure a clean slate before performing a full backfill.
-- This makes the operation idempotent.
TRUNCATE TABLE actors_history_scd;

-- Use a single transaction block to ensure the TRUNCATE and INSERT are atomic.
BEGIN;

INSERT INTO actors_history_scd (
    actorid,
    actor,
    quality_class,
    is_active,
    start_year,
    end_year,
    current_year
)
WITH 
-- Step 1: Create a snapshot of each actor's status for each year.
yearly_actor_snapshots AS (
  SELECT
    a.actorid,
    a.actor,
    y.year,
    -- Determine quality_class based on the average rating of films *in this year*.
    CASE 
      WHEN AVG(a.rating) > 8 THEN 'star'
      WHEN AVG(a.rating) > 7 THEN 'good'
      WHEN AVG(a.rating) > 6 THEN 'average'
      ELSE 'bad'
    END::quality_class_e AS quality_class,
    -- Determine if the actor was active *in this year*.
    (COUNT(a.year) > 0) AS is_active
  FROM actor_films a
  -- Generate a series of years to create a snapshot for each one.
  CROSS JOIN (SELECT generate_series(1970, 2021) AS year) y
  WHERE a.year = y.year
  GROUP BY a.actorid, a.actor, y.year
),

-- Step 2: Detect changes between yearly snapshots to identify streaks.
with_streaks AS (
  SELECT
    *,
    -- Use SUM() as a window function over a change indicator. This clever trick
    -- assigns a unique ID to each continuous period of unchanged data.
    SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY year) AS streak_identifier
  FROM (
    SELECT
      *,
      -- Use LAG() to get the previous year's values for comparison.
      -- A change_indicator of 1 means the streak was broken.
      CASE
        WHEN quality_class <> LAG(quality_class, 1, quality_class) OVER (PARTITION BY actorid ORDER BY year) THEN 1
        WHEN is_active <> LAG(is_active, 1, is_active) OVER (PARTITION BY actorid ORDER BY year) THEN 1
        ELSE 0
      END AS change_indicator
    FROM yearly_actor_snapshots
  )
)

-- Step 3: Consolidate the identified streaks into final SCD2 rows.
SELECT 
  actorid,
  actor,
  quality_class,
  is_active,
  -- The first year in the streak is the start_year.
  MIN(year) AS start_year,
  -- The last year in the streak is the end_year.
  MAX(year) AS end_year,
  -- Hardcode current_year to mark this as the 2021 backfill snapshot.
  2021 AS current_year
FROM with_streaks
GROUP BY actorid, actor, quality_class, is_active, streak_identifier;

COMMIT;


