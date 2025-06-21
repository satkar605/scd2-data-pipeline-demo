# Data Engineering Bootcamp: Dimensional Modeling Homework

This project demonstrates core data engineering principles by building a historical data model for actors' careers. It progresses from basic table creation to a production-grade, incremental pipeline for maintaining a Type 2 Slowly Changing Dimension (SCD) table.

## 1. Business Context & Analytical Purpose

### The Problem
Imagine you are a data analyst at a film industry publication. You have a raw dataset of every film an actor has been in and the year it was released. Your editor asks:

*   "How many actors transitioned from being 'average' to 'star' performers over the last decade?"
*   "Show me a list of actors who were consistently 'star' quality throughout the 1990s."
*   "What was Harrison Ford's career status and filmography back in 1985?"

A simple, flat table of actors cannot answer these questions because it only knows an actor's *current* state. It has no memory.

### The Solution
This project solves the problem by building a **dimensional data model** that captures history. Specifically, we build `actors_history_scd`, a Type 2 SCD table. This table keeps a versioned record of an actor's status (`quality_class` and `is_active`) over time, allowing for powerful historical analysis.

## 2. Technical Breakdown & File Descriptions

The project is broken down into five sequential SQL scripts. They must be run in order to correctly build the data model and populate it with data.

### Execution Order

1.  `t1_ddl.sql`
2.  `t2_cumulative.sql`
3.  `t3_scd2_table.sql`
4.  `t4_backfill.sql`
5.  `t5_incremental.sql`

---

### `t1_ddl.sql`: Core Data Model & Types

*   **Purpose:** Defines the foundational schema for the `actors` table, which is a simple Type 1 SCD (it holds only one row per actor).
*   **Key Concepts:**
    *   **Custom Types:** Creates a `films` composite type to store film data cleanly in an array and a `quality_class_e` ENUM to enforce data integrity for actor ratings.
    *   **Idempotency:** Uses `DROP TYPE IF EXISTS` and `DROP TABLE IF EXISTS` to ensure the script can be run multiple times without errors.

### `t2_cumulative.sql`: Initial (Type 1) Table Load

*   **Purpose:** Populates the `actors` table.
*   **Key Concepts:**
    *   **Procedural Loop (`DO $$`):** Iterates from year 1970 to 2021, calculating each actor's state as of that year.
    *   **`ON CONFLICT DO NOTHING`:** This is a crucial detail. Because of this clause, an actor is only inserted during the *first* year they appear. This means the `actors` table ends up storing each actor's **debut-year state**, not their most recent state.

### `t3_scd2_table.sql`: Historical Table DDL

*   **Purpose:** Defines the schema for the `actors_history_scd` table, the core of our historical model.
*   **Key Concepts:**
    *   **SCD2 Columns:** Introduces `start_year` and `end_year` to track the time window for which a version of a record was valid. `current_year` is added for partitioning and easier debugging of incremental loads.
    *   **Composite Primary Key:** The primary key `(actorid, start_year)` allows multiple historical versions for each actor, but only one version can start in a given year.

### `t4_backfill.sql`: Full Historical Backfill (SCD2)

*   **Purpose:** Fills the `actors_history_scd` table with the complete, versioned history of every actor from 1970 to 2021.
*   **Key Concepts:**
    *   **Yearly Snapshots:** The logic first calculates every actor's status for every single year.
    *   **Change Detection (`LAG`)**: The `LAG()` window function is used to compare a year's data to the previous year, flagging any changes.
    *   **Streak Identification (`SUM()`):** A windowed `SUM()` over the change flags assigns a unique ID to each continuous "streak" where an actor's status didn't change.
    *   **Atomicity (`BEGIN`/`COMMIT`):** The entire operation is wrapped in a transaction. It first `TRUNCATE`s the table and then inserts. If the insert fails, the truncate is rolled back, preventing data loss.

### `t5_incremental.sql`: Production-Grade Incremental Load

*   **Purpose:** This is the most important script. It demonstrates how to efficiently update the SCD table with a new year's worth of data (2022) without reprocessing the entire history.
*   **Key Concepts:**
    *   **Staging Table (`CREATE TEMP TABLE`):** To avoid CTE scope issues, the change analysis is first stored in a temporary table.
    *   **`UPDATE` for Unchanged:** For actors whose data didn't change, their most recent record is updated by setting `end_year = 2022`. This correctly extends their historical streak.
    *   **`INSERT` for New/Changed:** A new row (with `start_year = 2022`) is inserted only for brand new actors or actors whose data changed.
    *   **Efficiency & Scalability:** This **`UPDATE`/`INSERT`** pattern is the professional standard. It is highly efficient and scalable because the work required does not grow as the historical table gets larger.
