-- Runs once, when the compose database volume is first created.
-- The test suite drops and recreates tables, so it gets its own database.
CREATE DATABASE tenders_test OWNER tender;
