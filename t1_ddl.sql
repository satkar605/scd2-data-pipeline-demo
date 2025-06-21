-- Module 1 - Homework (Par 1: DDL)
-- DDL
-- SELECT * FROM actor_films;

-- ## Assignment Tasks

-- 1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
--     - `films`: An array of `struct` with the following fields:
-- 		- film: The name of the film.
-- 		- votes: The number of votes the film received.
-- 		- rating: The rating of the film.
-- 		- filmid: A unique identifier for each film.

--     - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
-- 		- `star`: Average rating > 8.
-- 		- `good`: Average rating > 7 and ≤ 8.
-- 		- `average`: Average rating > 6 and ≤ 7.
-- 		- `bad`: Average rating ≤ 6.
--     - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).

-- Creating a table 'actors' that has the following fields: 'films' (a struct) + 'quality_class' (enum) + 'is_active' (BOOL)

-- Creating a custom composite type called 'films'
-- This type represents a STRUCT that groups film information together
-- CREATE TYPE films AS(
-- 				film TEXT,			-- Name of the film
-- 				votes INTEGER,		-- Number of votes the film received
-- 				rating REAL,		-- Film rating
-- 				filmid TEXT			-- Unique identifier for the film
-- )

/* Creating an ENUM type for 'quality_class' */
-- This ENUM defines the allowed categories for actor performance based on average ratings:
-- CREATE TYPE quality_class_e AS ENUM('star', 'good', 'average', 'bad');

-- /* Creating the 'actors' table */
-- -- The table stors each actor, their films, and additional classification
CREATE TABLE actors(
	actorid TEXT PRIMARY KEY,			-- Unique indentifier
	actor TEXT,							
	films films[],		
	quality_class quality_class_e,
	is_active BOOLEAN
)

-- DROP TABLE IF EXISTS actors;

