-- ===================================================================================
-- Task 3: DDL for the `actors_history_scd` Table
-- ===================================================================================
-- This script defines the Data Definition Language (DDL) for the `actors_history_scd`
-- table, which will store the complete versioned history of actors. This is a
-- classic Type 2 Slowly Changing Dimension (SCD) table.
--
-- The script is designed to be idempotent; running it multiple times will
-- result in the same clean table, ready for the backfill process.
-- ===================================================================================

-- Drop the table if it already exists to ensure a clean start.
DROP TABLE IF EXISTS actors_history_scd;

-- Create the table to store actor history.
CREATE TABLE actors_history_scd (
	actorid TEXT,							-- The unique identifier for an actor.
	actor TEXT,								-- The actor's name.
	quality_class quality_class_e,			-- The calculated quality class (e.g., 'star', 'average').
	is_active BOOLEAN,						-- A flag indicating if the actor was active during the period.
	start_year INTEGER,						-- The first year this version of the record was valid.
	end_year INTEGER,						-- The last year this version of the record was valid.
	current_year INTEGER,					-- The year this row was loaded, for partitioning and debugging.
	
	-- The primary key ensures that an actor can only have one version starting in a given year.
	PRIMARY KEY (actorid, start_year)
);