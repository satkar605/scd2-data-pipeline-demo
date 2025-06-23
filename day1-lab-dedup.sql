-- -- ===================================================================================
-- -- FACT TABLE MODELING: Identifying and Removing Duplicates from Raw Log Data
-- -- ===================================================================================
-- -- Objective:
-- -- Begin designing a fact table using 'game_details' as the raw source.
-- -- The first step is to address duplicate records, which would otherwise result in 
-- -- incorrect aggregations (double-counts, inflated metrics, etc.) in the fact table.

-- -- ===================================================================================

-- -- Step 1: Investigate duplicates at the desired fact grain level
-- -- The expected grain is one row per (game_id, team_id, player_id) combination.

-- -- SELECT 
-- -- 	game_id, team_id, player_id, COUNT(1)  -- COUNT(1) gives the number of rows for each group
-- -- FROM game_details
-- -- GROUP BY 1,2,3  -- Group data by game_id, team_id, player_id
-- -- HAVING COUNT(1) > 1  -- Show only combinations that appear more than once (duplicates)

-- -- ===================================================================================

-- -- Step 2: Perform deduplication using ROW_NUMBER()

-- WITH deduped AS (

-- 	SELECT
-- 		-- Assign a sequential row number within each partitioned group
-- 		-- PARTITION BY groups data at the desired fact grain level
-- 		-- Each duplicate group gets numbered 1, 2, 3, etc.
-- 		*, ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS row_num 
-- 	FROM game_details
	
-- )

-- -- Step 3: Keep only the first row from each group (row_num = 1)
-- -- This removes duplicates by retaining one row per (game_id, team_id, player_id) combination.
-- SELECT * 
-- FROM deduped
-- WHERE row_num = 1;

-- -- ===================================================================================
-- -- This becomes the clean starting point for downstream fact table modeling.
-- -- The logic ensures only unique rows per desired grain are passed forward.
-- -- ===================================================================================


-- -- ===================================================================================
-- -- FACT TABLE MODELING: Deduplicating Raw Log Data for Fact Table Load
-- -- ===================================================================================
-- -- Objective:
-- -- We are preparing a clean, deduplicated dataset from the raw 'game_details' log.
-- -- The desired grain for the fact table is one row per (game_id, team_id, player_id).
-- -- Duplicate rows violate this grain and must be resolved before inserting into the fact table.

-- -- Approach:
-- -- 1. Identify duplicates based on (game_id, team_id, player_id).
-- -- 2. Introduce temporal evidence using 'game_date_est' from the 'games' table.
-- -- 3. Apply ROW_NUMBER() to assign a sequence within each duplicate group.
-- -- 4. Keep only the first row from each group, ordered by game date, ensuring deterministic deduplication.

-- -- ===================================================================================

-- -- Common query while working with log to select the grain
-- WITH deduped AS (
	
-- 	SELECT 
-- 			-- Bring in game_date_est from games table to use as temporal evidence for ordering
-- 			g.game_date_est, 
-- 			-- Select all columns from game_details for downstream fact table modeling
-- 			gd.*,
			
-- 			-- Use ROW_NUMBER() to assign a sequential ID within each duplicate group
-- 			-- PARTITION BY defines the grain we expect: one row per (game_id, team_id, player_id)
-- 			-- ORDER BY game_date_est ensures that the earliest game_date_est is given row_num = 1
-- 			ROW_NUMBER () OVER (PARTITION BY gd.game_id, team_id, player_id 
-- 								ORDER BY g.game_date_est) as row_num 
-- 		FROM game_details gd
-- 			-- Join with games table to get access to game_date_est column
-- 			JOIN games g 
-- 			  ON gd.game_id = g.game_id
-- )

-- -- After assigning row numbers, select only the first row from each group
-- SELECT * 
-- FROM deduped
-- WHERE row_num = 1;

-- -- ===================================================================================
-- -- FACT TABLE MODELING: Extracting Clean Facts from Raw Game Log Data (Enriched Version)
-- -- ===================================================================================
-- -- Objective:
-- -- Continue building a clean, deduplicated fact dataset from raw 'game_details'.
-- -- This step enriches the fact model by adding derived dimensions, parsing raw text fields,
-- -- and shaping the fact grain for production readiness.

-- -- ===================================================================================

-- -- Step 1: Deduplicate using ROW_NUMBER(), introduce temporal ordering with game_date_est
-- WITH deduped AS (
	
-- 	SELECT 
-- 			-- Bring in temporal evidence for ordering
-- 			g.game_date_est, 
-- 			-- Season information useful for slicing analysis
-- 			g.season,
-- 			-- Bring in team context for home/visitor analysis
-- 			g.home_team_id, 
-- 			g.visitor_team_id,
-- 			-- Include all original game_details fields
-- 			gd.*,
			
-- 			-- Assign sequential row number within each duplicate group based on game_date_est
-- 			ROW_NUMBER () OVER (
-- 				PARTITION BY gd.game_id, team_id, player_id 
-- 				ORDER BY g.game_date_est
-- 			) AS row_num 
-- 		FROM game_details gd
-- 			JOIN games g 
-- 			  ON gd.game_id = g.game_id
-- 		-- Filter to a single game date for prototype and testing
-- 		WHERE g.game_date_est = '2016-10-14'
-- )

-- -- Step 2: Extract desired fact attributes and parse complex fields
-- SELECT 
-- 	-- Keep key natural dimensions
-- 	game_date_est,
-- 	season,
-- 	team_id,

-- 	-- Derive boolean flag: is this team playing at home?
-- 	team_id = home_team_id AS dim_is_playing_at_home, 
	
-- 	player_id,
-- 	player_name,
-- 	start_position,
	
-- 	-- Parse complex 'comment' text field into clean boolean flags
-- 	-- Using COALESCE to handle possible NULL values safely
-- 	COALESCE(POSITION('DNP' IN comment), 0) > 0 
-- 		AS dim_did_not_play,
-- 	COALESCE(POSITION('DND' IN comment), 0) > 0 
-- 		AS dim_did_not_dress,
-- 	COALESCE(POSITION('NWT' IN comment), 0) > 0 
-- 		AS dim_not_with_team,

-- 	-- Include core fact measures (atomic, non-derived values)
-- 	CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL)/60 AS minutes, -- this makes it easier for the analysts to derive stats per minute
-- 	fgm,
-- 	fga,
-- 	fg3m,
-- 	fg3a,
-- 	ftm,
-- 	fta,
-- 	oreb,
-- 	dreb,
-- 	reb,
-- 	ast,
-- 	stl,
-- 	blk,
-- 	"TO" AS turnovers,  -- Alias TO column to avoid reserved keyword conflicts
-- 	pf,
-- 	pts,
-- 	plus_minus

-- FROM deduped
-- -- Apply deduplication: keep only first row per group after ROW_NUMBER()
-- WHERE row_num = 1;

-- -- ===================================================================================
-- -- Key Notes:
-- -- - Fact model only keeps fundamental, atomic attributes.
-- -- - Derived fields like shooting percentages are intentionally excluded 
-- --   (can be derived downstream as needed).
-- -- - Complex text columns (like 'comment') are parsed into clean booleans for easier consumption.
-- -- ===================================================================================

-- ===================================================================================
-- FACT TABLE CREATION: fct_game_details
-- ===================================================================================
-- Objective:
-- Create a fact table to store cleaned and deduplicated player-game level data.
-- This table follows proper dimensional modeling conventions using:
-- - dim_ prefix for dimension attributes
-- - m_ prefix for measurable fact attributes (metrics)
-- - PRIMARY KEY enforcing fact grain at (game_date, team_id, player_id)

--DROP TABLE fct_game_details
CREATE TABLE fct_game_details (

	-- Dimension Columns (Descriptive Attributes)
	dim_game_date DATE,               -- Game date (natural time dimension)
	dim_season INTEGER,               -- Season identifier
	dim_team_id INTEGER,              -- Team identifier
	dim_player_id INTEGER,            -- Player identifier
	dim_player_name TEXT,             -- Player full name
	dim_start_position TEXT,          -- Player's starting position (categorical: e.g. Guard, Forward, etc.)
	dim_is_playing_at_home BOOLEAN,   -- Boolean flag: is this player playing at home?
	dim_did_not_play BOOLEAN,         -- Derived flag from comment text: Did Not Play (DNP)
	dim_did_not_dress BOOLEAN,        -- Derived flag from comment text: Did Not Dress (DND)
	dim_not_with_team BOOLEAN,        -- Derived flag from comment text: Not With Team (NWT)

	-- Measure Columns (Numeric Facts)
	m_min REAL,                   -- Total minutes played
	m_fgm INTEGER,                    -- Field Goals Made
	m_fga INTEGER,                    -- Field Goals Attempted
	m_fg3m INTEGER,                   -- 3-Point Field Goals Made
	m_fg3a INTEGER,                   -- 3-Point Field Goals Attempted
	m_ftm INTEGER,                    -- Free Throws Made
	m_fta INTEGER,                    -- Free Throws Attempted
	m_oreb INTEGER,                   -- Offensive Rebounds
	m_dreb INTEGER,                   -- Defensive Rebounds
	m_reb INTEGER,                    -- Total Rebounds
	m_ast INTEGER,                    -- Assists
	m_stl INTEGER,                    -- Steals
	m_blk INTEGER,                    -- Blocks
	m_turnovers INTEGER,              -- Turnovers
	m_pf INTEGER,                     -- Personal Fouls
	m_pts INTEGER,                    -- Total Points Scored
	m_plus_minus INTEGER,             -- Plus-Minus metric (point differential while on court)

	-- Primary Key enforcing the fact table grain
	PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)

);

-- ===================================================================================
-- This fact table is now ready to receive fully cleaned, deduplicated, 
-- and properly modeled data coming from your earlier deduplication query.
-- ===================================================================================
-- ===================================================================================
-- FACT TABLE LOAD: Insert Cleaned and Modeled Data into fct_game_details
-- ===================================================================================
-- Logic Summary:
-- - This query extracts cleaned, deduplicated, and fully modeled fact-level data 
--   from raw log tables (game_details and games).
-- - Deduplication is performed using ROW_NUMBER() with partitioning on 
--   (game_id, team_id, player_id), ensuring one unique row per player-game-team combination.
-- - Temporal ordering (game_date_est) is used inside ROW_NUMBER() to ensure deterministic deduplication.
-- - The 'min' column, originally stored as a string in 'MM:SS' format, is parsed and converted into 
--   numeric total minutes (as m_min) for proper aggregation in the fact table.
-- - Comment column (text flags like DNP, DND, NWT) is parsed into clean boolean flags (dim_did_not_play, etc.)
-- - Column naming follows strict dimensional modeling conventions:
--     * dim_ prefix for dimensions (categorical attributes)
--     * m_ prefix for measures (numeric facts)
-- - The final result is inserted directly into the fct_game_details fact table.

-- ===================================================================================

INSERT INTO fct_game_details
WITH deduped AS (
	SELECT 
			g.game_date_est, 
			g.season,
			g.home_team_id, 
			g.visitor_team_id,
			gd.*,
			ROW_NUMBER() OVER (
				PARTITION BY gd.game_id, gd.team_id, gd.player_id 
				ORDER BY g.game_date_est
			) AS row_num 
	FROM game_details gd
		JOIN games g 
		  ON gd.game_id = g.game_id
)

SELECT 
	game_date_est AS dim_game_date,
	season AS dim_season,
	team_id AS dim_team_id,
	player_id AS dim_player_id,
	player_name AS dim_player_name,
	start_position AS dim_start_position,
	(team_id = home_team_id) AS dim_is_playing_at_home,

	(COALESCE(POSITION('DNP' IN comment), 0) > 0) AS dim_did_not_play,
	(COALESCE(POSITION('DND' IN comment), 0) > 0) AS dim_did_not_dress,
	(COALESCE(POSITION('NWT' IN comment), 0) > 0) AS dim_not_with_team,

	CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL)/60 AS m_min,

	fgm AS m_fgm,
	fga AS m_fga,
	fg3m AS m_fg3m,
	fg3a AS m_fg3a,
	ftm AS m_ftm,
	fta AS m_fta,
	oreb AS m_oreb,
	dreb AS m_dreb,
	reb AS m_reb,
	ast AS m_ast,
	stl AS m_stl,
	blk AS m_blk,
	"TO" AS m_turnovers,  
	pf AS m_pf,
	pts AS m_pts,
	plus_minus AS m_plus_minus

FROM deduped
WHERE row_num = 1;

-- ===================================================================================
-- ANALYTICAL QUERY #1: Join fact table with team dimension
-- ===================================================================================
-- Goal: Enrich fact-level data with team attributes.
-- Logic: Simple inner join using team_id foreign key.
-- Joins are extremely fast because dimension tables (like teams) are small.

SELECT 
	t.*, 
	gd.*
FROM fct_game_details gd 
	JOIN teams t
	  ON t.team_id = gd.dim_team_id;

-- ===================================================================================
-- ANALYTICAL QUERY #2: Find players who bailed out on the most games
-- ===================================================================================
-- Goal: Analyze player behavior using clean dimensional flags.
-- Specifically calculate how often players were 'Not With Team' (NWT).

SELECT 
	dim_player_name,                                 -- Player name (dimension)
	COUNT(1) AS num_games,                           -- Total games recorded
	COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num, -- Games where player was not with team
	CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL)
		/ COUNT(1) AS bail_pct                      -- Proportion of games bailed
FROM fct_game_details
GROUP BY 1
ORDER BY 4 DESC;  -- Sort by highest bailout percentage

-- ===================================================================================
-- COMMENTARY:
-- - This query leverages the clean boolean dimension dim_not_with_team.
-- - The dimensional model enables simple aggregations without parsing raw text.
-- - These are the exact kinds of clean queries dimensional models are designed for.
-- ===================================================================================
