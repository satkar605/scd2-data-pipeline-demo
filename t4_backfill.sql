-- ============================================================
-- Task 3: SCD Type 2 Backfill for Actors History
-- ============================================================
-- This query generates full historical version tracking for actors.
-- 
-- Step-by-step breakdown:
-- 1️⃣ Generate all target years from 1970 to 2021 (generate_series).
-- 2️⃣ For each year, pull cumulative film data up to that year.
-- 3️⃣ Aggregate average rating and film count per actor per year.
-- 4️⃣ Assign quality_class based on yearly avg_rating.
-- 5️⃣ Mark is_active if actor had at least one film in that year.
-- 6️⃣ Use LAG() to compare current vs previous year's attributes.
-- 7️⃣ Flag any changes using change_indicator.
-- 8️⃣ Use SUM OVER() to generate streak_identifier for continuous unchanged periods.
-- 9️⃣ Aggregate streaks into SCD version rows (start_year, end_year).
-- 🔟 Finally, insert fully versioned data into actors_history_scd table.
-- ============================================================

WITH years AS (
  SELECT generate_series(1970, 2021) AS year
),

filtered_films AS (
  SELECT
    af.actorid,
    af.actor,
    af.film,
    af.votes,
    af.rating,
    af.filmid,
    af.year,
    y.year AS target_year
  FROM actor_films af
  CROSS JOIN years y
  WHERE af.year <= y.year
),

aggregated_films AS (
  SELECT
    actorid,
    actor,
    target_year AS year,
    AVG(rating) FILTER (WHERE year = target_year) AS avg_rating,
    COUNT(*) FILTER (WHERE year = target_year) AS film_count
  FROM filtered_films
  GROUP BY actorid, actor, target_year
),

yearly_actor_snapshots AS (
  SELECT
    actorid,
    actor,
    year,
    CASE 
      WHEN avg_rating > 8 THEN 'star'
      WHEN avg_rating > 7 THEN 'good'
      WHEN avg_rating > 6 THEN 'average'
      ELSE 'bad'
    END::quality_class_e AS quality_class,
    (film_count > 0) AS is_active
  FROM aggregated_films
),

with_previous AS (
  SELECT
    actorid,
    actor,
    year,
    quality_class,
    is_active,
    LAG(quality_class) OVER (PARTITION BY actorid ORDER BY year) AS previous_quality_class,
    LAG(is_active) OVER (PARTITION BY actorid ORDER BY year) AS previous_is_active
  FROM yearly_actor_snapshots
),

with_indicators AS (
  SELECT *,
    CASE
      WHEN quality_class <> previous_quality_class THEN 1
      WHEN is_active <> previous_is_active THEN 1
      ELSE 0
    END AS change_indicator
  FROM with_previous
),

with_streaks AS (
  SELECT *,
    SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY year) AS streak_identifier
  FROM with_indicators
),

final_scd AS (
  SELECT 
    actorid,
    actor,
    quality_class,
    is_active,
    MIN(year) AS start_year,
    MAX(year) AS end_year,
    2021 AS current_year
  FROM with_streaks
  GROUP BY actorid, actor, quality_class, is_active, streak_identifier
)

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
    start_year,
    end_year,
    current_year
FROM final_scd;


