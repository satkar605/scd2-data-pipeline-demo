-- ---------------------------------------------------
-- Task 3 Start: SCD Type 2 Table Creation
-- ---------------------------------------------------
-- Creating 'actors_history_scd' table to capture slowly changing attributes.
-- 1. Each actor can have multiple records across years.
-- 2. start_year: year when the attribute version becomes active.
-- 3. end_year: reserved for closing out old versions (future step).
-- 4. current_year: tracks which year this row was generated from.
-- 5. PRIMARY KEY: (actorid, start_year) allows multiple versions per actor.
-- ---------------------------------------------------
CREATE TABLE actors_history_scd (
	actorid TEXT,
	actor TEXT,
	quality_class quality_class_e,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER,
	PRIMARY KEY (actorid, start_year)
);