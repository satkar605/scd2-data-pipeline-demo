-- Full cumulative load for 'actors' table (Dimensional Data Modeling - Week 1 HW)
-- ---------------------------------------------------
-- Logic overview:
-- 1. Loop through each year from 1970 to 2021.
-- 2. For each year:
--    a. Filter all films released up to that year.
--    b. Aggregate films into ARRAY of STRUCTs per actor.
--    c. Compute avg rating for films released exactly in that year.
--    d. Classify actors into quality_class based on avg rating.
--    e. Mark is_active if actor has films in that year.
-- 3. Insert new actors into 'actors' table.
-- 4. Skip existing actors (no updates yet, strictly inserts only).
-- ---------------------------------------------------
-- This version handles purely cumulative insertion logic.
-- (Array accumulation & full SCD versioning handled separately.)

DO $$
DECLARE
  loop_year INTEGER;
BEGIN
  FOR loop_year IN 1970..2021 LOOP
    INSERT INTO actors (actorid, actor, films, quality_class, is_active)
    WITH year_input AS (
        SELECT loop_year AS target_year  -- feed procedural variable into CTE safely
    ),
    filtered_films AS (
        SELECT
            actorid,
            actor,
            film,
            votes,
            rating,
            filmid,
            year
        FROM actor_films af
        JOIN year_input yi
          ON af.year <= yi.target_year
    ),
    aggregated_films AS (
        SELECT
            actorid,
            actor,
            ARRAY_AGG(
                ROW(film, votes, rating, filmid)::films
                ORDER BY year DESC
            ) AS films
        FROM filtered_films
        GROUP BY actorid, actor
    ),
    actor_metrics AS (
        SELECT 
            af.actorid,
            af.actor,
            af.films,
            ratings.avg_rating,
            ratings.film_count
        FROM aggregated_films af
        LEFT JOIN (
            SELECT
                actorid,
                AVG(rating) AS avg_rating,
                COUNT(*) AS film_count
            FROM filtered_films
            WHERE year = (SELECT target_year FROM year_input)
            GROUP BY actorid
        ) ratings
        ON af.actorid = ratings.actorid
    ),
    final_actors AS (
        SELECT 
            actorid, 
            actor, 
            films,
            CASE
                WHEN avg_rating > 8 THEN 'star'
                WHEN avg_rating > 7 THEN 'good'
                WHEN avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class_e AS quality_class,
            (film_count > 0) AS is_active
        FROM actor_metrics
    )
    SELECT * FROM final_actors
    ON CONFLICT (actorid) DO NOTHING;
  END LOOP;
END $$;







