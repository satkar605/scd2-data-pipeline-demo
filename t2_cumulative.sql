-- ===================================================================================
-- Task 2: Cumulative Load for the `actors` Table
-- ===================================================================================
-- This script populates the `actors` table, which is a Type 1 SCD table intended
-- to hold a single, current-state row per actor.
--
-- Logic:
-- The script uses a procedural loop in PostgreSQL (`DO` block) to iterate through
-- each year from 1970 to 2021. For each year, it calculates a full snapshot of
-- every actor's state *as of that year*.
--
-- Key Behavior - ON CONFLICT DO NOTHING:
-- The `INSERT` statement uses `ON CONFLICT (actorid) DO NOTHING`. This means that
-- an actor's row will be inserted only during the loop corresponding to the
-- *first year they ever appeared* in the `actor_films` data. For all subsequent
-- years, the `INSERT` will fail silently for that actor. As a result, this script
-- populates the table with actor data based on their debut year, not their most
-- recent year. This is likely a deliberate step in the bootcamp curriculum
-- before introducing upsert (`DO UPDATE`) logic.
-- ===================================================================================

-- A DO block allows for procedural code, like loops, to be executed in psql.
DO $$
DECLARE
  loop_year INTEGER;
BEGIN
  -- Loop through each year from 1970 to 2021 to build a historical state.
  FOR loop_year IN 1970..2021 LOOP
    INSERT INTO actors (actorid, actor, films, quality_class, is_active)
    WITH 
    -- CTE to safely pass the current loop_year into the main query.
    year_input AS (
        SELECT loop_year AS target_year
    ),
    -- CTE to gather all films for each actor up to and including the target_year.
    -- This creates a cumulative view of an actor's filmography.
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
    -- CTE to aggregate the cumulative films into a single array for each actor.
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
    -- CTE to calculate metrics for an actor based *only* on the target_year.
    -- This determines their `quality_class` and `is_active` status for that year.
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
    -- Final CTE to construct the complete actor row for the target_year.
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
            -- An actor is considered active if they had any films in the target_year.
            (film_count > 0) AS is_active
        FROM actor_metrics
    )
    SELECT * FROM final_actors
    -- If an actor already exists in the table, do nothing.
    -- This is the key part that causes the table to reflect the actor's debut year state.
    ON CONFLICT (actorid) DO NOTHING;
  END LOOP;
END $$;







