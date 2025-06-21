-- ===================================================================================
-- Task 1: DDL for Core Actor Data Model (`actors` table and custom types)
-- ===================================================================================
-- This script defines the foundational Data Definition Language (DDL) for the `actors`
-- table and its associated custom types. This table is a Type 1 SCD, designed to
-- hold a single, current-state row per actor.
--
-- The script is idempotent; it safely drops and recreates types and tables to
-- ensure a clean state upon every execution.
-- ===================================================================================

-- Drop dependent objects first in reverse order of creation to avoid errors.
DROP TABLE IF EXISTS actors;
DROP TYPE IF EXISTS quality_class_e;
DROP TYPE IF EXISTS films;


-- Create a custom composite type to represent a single film's data.
-- This allows us to store an array of these structs in the `actors` table.
CREATE TYPE films AS (
    film TEXT,          -- Name of the film.
    votes INTEGER,      -- Number of votes the film received.
    rating REAL,        -- Film rating.
    filmid TEXT         -- Unique identifier for the film.
);

-- Create a custom ENUM type to constrain the values of `quality_class`.
-- This enforces data integrity by ensuring only these specific values can be used.
CREATE TYPE quality_class_e AS ENUM (
    'star',
    'good',
    'average',
    'bad'
);

-- Create the main 'actors' table.
CREATE TABLE actors (
    actorid TEXT PRIMARY KEY,           -- Unique identifier for the actor, serving as the primary key.
    actor TEXT,                         -- The actor's name.
    films films[],                      -- An array of the custom 'films' type, storing the actor's filmography.
    quality_class quality_class_e,      -- The actor's calculated quality class, constrained by the ENUM.
    is_active BOOLEAN                   -- A flag indicating if the actor is currently active.
);

-- DROP TABLE IF EXISTS actors;

