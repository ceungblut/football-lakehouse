-- Unity Catalog grants (template)
-- Goal: keep a simple, safe default that works for a solo developer,
-- while providing an obvious path to add groups/service principals later.
--
-- Notes:
-- - Replace `your_user_or_group` placeholders with real principals.
-- - In Unity Catalog, principals are typically users, groups, or service principals.
-- - Prefer granting to groups rather than individual users once collaboration starts.

USE CATALOG football;

-- ============================================================
-- 1) Minimal solo default (safe)
-- ============================================================
-- If you're the owner/creator of the catalog & schemas, you already have privileges.
-- This section is intentionally empty to avoid breaking forks or applying incorrect grants.

-- ============================================================
-- 2) Recommended role-based grants (uncomment and edit when needed)
-- ============================================================

-- -- Create a few logical groups in your IdP / Databricks:
-- --   football-admins    : full control
-- --   football-engineers : build pipelines and write tables
-- --   football-readers   : read-only access to Gold (and optionally Silver)

-- -- Catalog level
-- GRANT USE CATALOG ON CATALOG football TO `football-engineers`;
-- GRANT USE CATALOG ON CATALOG football TO `football-readers`;

-- -- Schema usage
-- GRANT USE SCHEMA ON SCHEMA football.bronze TO `football-engineers`;
-- GRANT USE SCHEMA ON SCHEMA football.silver TO `football-engineers`;
-- GRANT USE SCHEMA ON SCHEMA football.gold   TO `football-engineers`;

-- GRANT USE SCHEMA ON SCHEMA football.gold   TO `football-readers`;
-- -- Optional: allow readers to see Silver too
-- -- GRANT USE SCHEMA ON SCHEMA football.silver TO `football-readers`;

-- -- Table privileges (engineers)
-- -- Bronze: engineers can write + read (ingestion + debugging)
-- GRANT SELECT, MODIFY ON SCHEMA football.bronze TO `football-engineers`;
-- -- Silver/Gold: engineers can build and read
-- GRANT SELECT, MODIFY ON SCHEMA football.silver TO `football-engineers`;
-- GRANT SELECT, MODIFY ON SCHEMA football.gold   TO `football-engineers`;

-- -- Table privileges (readers)
-- -- Readers: read-only Gold by default
-- GRANT SELECT ON SCHEMA football.gold TO `football-readers`;
-- -- Optional: readers can read Silver too
-- -- GRANT SELECT ON SCHEMA football.silver TO `football-readers`;

-- ============================================================
-- 3) Optional: tighter, table-level grants (more explicit)
-- ============================================================
-- Use this if you want to restrict access to specific tables rather than schema-wide.

-- -- Example: allow readers to query only the published dimensions and facts
-- GRANT SELECT ON TABLE football.gold.dim_player TO `football-readers`;
-- GRANT SELECT ON TABLE football.gold.dim_team TO `football-readers`;
-- GRANT SELECT ON TABLE football.gold.dim_gameweek TO `football-readers`;
-- GRANT SELECT ON TABLE football.gold.dim_fixture TO `football-readers`;
-- GRANT SELECT ON TABLE football.gold.fact_price_snapshot TO `football-readers`;
-- GRANT SELECT ON TABLE football.gold.fact_player_gameweek TO `football-readers`;
-- GRANT SELECT ON TABLE football.gold.fact_live_form TO `football-readers`;
-- GRANT SELECT ON TABLE football.gold.fact_transfer_recommendation TO `football-readers`;

-- ============================================================
-- 4) Ownership / administration (uncomment when you have an admin group)
-- ============================================================
-- Admins typically manage objects and grants.
-- In UC, ownership is important—consider transferring ownership to an admin group.

-- -- Example: admins can manage everything
-- GRANT ALL PRIVILEGES ON CATALOG football TO `football-admins`;
-- GRANT ALL PRIVILEGES ON SCHEMA football.bronze TO `football-admins`;
-- GRANT ALL PRIVILEGES ON SCHEMA football.silver TO `football-admins`;
-- GRANT ALL PRIVILEGES ON SCHEMA football.gold   TO `football-admins`;

-- ============================================================
-- 5) Operational notes (keep for forks)
-- ============================================================
-- - Prefer granting at schema level during development for simplicity.
-- - Tighten to table-level grants if/when you expose the dataset to others.
-- - Avoid granting MODIFY to readers.
-- - For production, use groups + service principals rather than personal users.

