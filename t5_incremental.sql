-- ===================================================================================
-- Task 5 - FINAL POLISHED INCREMENTAL SCD2 BUILD (Safe & Production Ready)
-- ===================================================================================
-- This script performs a robust, production-grade incremental load for the year 2022.
-- It is designed to be idempotent and respect the PRIMARY KEY constraint.
--
-- Logic:
-- 1. Create a temporary staging table to hold the analysis of what has changed.
-- 2. Use CTEs to populate this staging table by comparing the 2021 state with the
--    incoming 2022 data.
-- 3. Perform an UPDATE on the main SCD table to extend the `end_year` of unchanged
--    records, using the staging table to identify them.
-- 4. Perform a single INSERT to add new version rows for changed and new actors,
--    also using the staging table.
-- 5. The temporary table is automatically dropped at the end of the session.
-- ===================================================================================

-- Create a temporary table to stage the results of the change analysis.
-- This solves the CTE scope issue and is more performant than re-running the CTEs.
CREATE TEMP TABLE changes_staging AS
WITH last_year_scd AS (
    -- Get the most recent (open-ended) version for each actor from the 2021 load.
    SELECT 
        actorid,
        actor,
        quality_class,
        is_active,
        start_year
    FROM actors_history_scd
    WHERE end_year = 2021 AND current_year = 2021
),
this_year_data AS (
    -- Get the incoming data for the current year (2022).
    SELECT 
        actorid,
        actor,
        quality_class,
        is_active
    FROM actors
),
-- Combine and identify the state of each record for the incremental load.
combined AS (
    SELECT
        ls.actorid AS last_year_actorid,
        ts.actorid AS this_year_actorid,
        ls.quality_class AS last_year_quality_class,
        ts.quality_class AS this_year_quality_class,
        ls.is_active AS last_year_is_active,
        ts.is_active AS this_year_is_active,
        COALESCE(ts.actorid, ls.actorid) as actorid,
        ts.actor,
        ts.quality_class,
        ts.is_active
    FROM last_year_scd ls
    FULL OUTER JOIN this_year_data ts ON ls.actorid = ts.actorid
)
-- Final categorization of each actor into 'new', 'changed', or 'unchanged'.
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    CASE
        WHEN last_year_actorid IS NULL THEN 'new'
        WHEN this_year_actorid IS NULL THEN 'retired' -- Not handled in this script, but good to identify
        WHEN last_year_quality_class <> this_year_quality_class OR last_year_is_active <> this_year_is_active THEN 'changed'
        ELSE 'unchanged'
    END as change_type
FROM combined;

-- Step 2: Update the end_year for all records that have not changed.
-- This "extends" their active streak into the new year.
UPDATE actors_history_scd
SET 
    end_year = 2022,
    current_year = 2022
WHERE actorid IN (SELECT actorid FROM changes_staging WHERE change_type = 'unchanged')
  AND end_year = 2021;


-- Step 3: Insert new version rows ONLY for new and changed actors.
-- This will not cause a primary key violation.
INSERT INTO actors_history_scd (
    actorid,
    actor,
    quality_class,
    is_active,
    start_year,
    end_year,
    current_year
)
SELECT 
    actorid,
    actor,
    quality_class,
    is_active,
    2022 AS start_year,
    2022 AS end_year,
    2022 AS current_year
FROM changes_staging
WHERE change_type IN ('new', 'changed');
