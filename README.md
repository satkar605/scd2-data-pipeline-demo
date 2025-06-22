# Data Engineering Bootcamp: Dimensional Modeling Project

This report documents the design and implementation of a historical dimensional data model for tracking actors’ careers. The project applies core data engineering principles including schema design, type system utilization, cumulative data ingestion, and Slowly Changing Dimension Type 2 (SCD2) modeling.

## 1. Business Objective

The film industry continuously analyzes actor performance trends to support talent management, content strategy, and historical reporting. Traditional flat data structures only capture an actor's most recent status, limiting the ability to answer questions about historical patterns such as:

- The number of actors who advanced from *average* to *star* quality over time
- Actors who consistently maintained *star* quality during specific periods
- An actor’s historical status and filmography at any given point in their career

This project addresses these limitations by designing a dimensional model capable of tracking actor status longitudinally. The solution enables both point-in-time reporting and longitudinal analysis for trend evaluation, career trajectory studies, and retrospective business insights.

## 2. Analytical Solution Overview

The solution consists of building a historical data model that transforms raw transactional data into an analyzable structure aligned with dimensional modeling best practices. Specifically, the model includes:

- An **actors** table that aggregates an actor’s film data at a given point in time
- An **actors_history_scd** table that captures historical changes to an actor’s status over time, using Slowly Changing Dimension Type 2 (SCD2) logic

This layered architecture supports both current-state analysis and detailed historical reporting.

## 3. Technical Implementation

The solution was delivered through a structured sequence of SQL scripts, executed in stages to build, populate, and maintain the dimensional model.

### 3.1 Data Modeling: Schema and Type Definitions

The first stage defined the core data structures required for the model:

- **Composite Type (`film_struct`)**  
  Designed to encapsulate film details (film name, votes, rating, film ID) as a nested array structure, enabling efficient aggregation of film data per actor.

- **Enumeration Type (`quality_class`)**  
  Enforces consistent categorization of actors based on average rating:  
  - *star* (average rating > 8)  
  - *good* (7 < rating ≤ 8)  
  - *average* (6 < rating ≤ 7)  
  - *bad* (rating ≤ 6)

- **Actors Table (`actors`)**  
  Stores one consolidated row per actor, including film history, quality classification, and active status.

This foundation allows for compact storage of an actor’s multi-film record while maintaining referential integrity and enforcing business rules at the data type level.

### 3.2 Cumulative Data Loading (Initial Population)

The **cumulative insert process** was designed to simulate annual snapshots of actor data, ensuring that only debut-year records were inserted into the `actors` table. This was achieved using:

- **Procedural Loop (`DO $$` block):** Iterates over each year to process actor debuts.
- **Conflict Management (`ON CONFLICT DO NOTHING`):** Ensures idempotency by inserting each actor only during the first year they appear.

This cumulative loading process serves as the basis for building historical context.

### 3.3 Historical SCD2 Table Design

The `actors_history_scd` table was engineered to track historical changes over time, capturing an actor’s:

- `quality_class`  
- `is_active` status  
- Validity period using `start_year` and `end_year` fields

A composite primary key `(actorid, start_year)` was implemented to ensure uniqueness per version.

This design supports precise time-window queries, allowing analytical users to reconstruct an actor's state at any historical point.

### 3.4 Historical Backfill (Full SCD2 Build)

The full backfill process calculated the complete historical record for all actors across the available time window (1970–2021). Key methods included:

- **Change Detection:** Using window function `LAG()` to compare yearly snapshots and detect status changes.
- **Streak Grouping:** Using cumulative `SUM()` to assign unique IDs to continuous periods where status remained unchanged.
- **Transactional Safety (`BEGIN` / `COMMIT`):** Ensured data integrity during the backfill process.

This process enabled the creation of fully versioned actor histories, laying the groundwork for longitudinal business analysis.

### 3.5 Incremental Processing (Production-Grade Pipeline)

An incremental load process was developed to simulate real-world, ongoing data ingestion:

- **Staging Area (Temporary Table):** Prepares change detection results before final updates.
- **Efficient Updates:**  
  - Existing records with unchanged attributes are updated by extending the `end_year`.
  - New rows are inserted for newly observed actors or those with attribute changes.

This approach ensures scalable, efficient maintenance of the historical dimension without full table reloads as new data arrives. It demonstrates enterprise-grade data engineering patterns that balance update efficiency with historical accuracy.

## 4. Business Value Delivered

The final dimensional model supports a broad range of analytical use cases, including:

- **Trend Analysis:** Tracking career progressions and regressions across time.
- **Historical Snapshotting:** Providing accurate actor status as of any historical period.
- **Cohort Analysis:** Identifying cohorts of actors with similar career trajectories.
- **Talent Management Insights:** Supporting data-driven decisions for casting, contracts, and production investment.

The solution transforms raw transactional data into a business-ready historical warehouse capable of powering advanced reporting, dashboards, and predictive modeling across multiple film industry stakeholders.

> *This model showcases foundational data engineering skills including schema design, type system design, cumulative loading, and robust Slowly Changing Dimension Type 2 modeling — forming a repeatable pattern applicable to many industries beyond entertainment.*
