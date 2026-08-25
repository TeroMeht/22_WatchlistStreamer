-- =========================================================================
-- 22_WatchlistStreamer -- rename persistent-table columns to lowercase.
--
-- Ephemeral tables (<sym>_livestream) are dropped and recreated at
-- every startup, so their schema catches up on the next boot with no
-- SQL needed here. This script only covers tables that persist across
-- sessions.
--
-- Safe to re-run: uses `IF EXISTS` on every rename.
--
-- Run once against BOTH databases:
--     livestreaming
--     livestreaming_replay
-- =========================================================================

BEGIN;

-- bars_2m_archive ---------------------------------------------------------
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Symbol"     TO symbol;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Date"       TO date;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Time"       TO time;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Open"       TO open;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "High"       TO high;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Low"        TO low;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Close"      TO close;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Volume"     TO volume;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "VWAP"       TO vwap;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "EMA9"       TO ema9;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Avg_volume" TO avg_volume;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Rvol"       TO rvol;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "Relatr"     TO relatr;
ALTER TABLE IF EXISTS bars_2m_archive RENAME COLUMN "DayAtrExt"  TO day_atr_ext;

-- alarms ------------------------------------------------------------------
ALTER TABLE IF EXISTS alarms RENAME COLUMN "Id"     TO id;
ALTER TABLE IF EXISTS alarms RENAME COLUMN "Symbol" TO symbol;
ALTER TABLE IF EXISTS alarms RENAME COLUMN "Time"   TO time;
ALTER TABLE IF EXISTS alarms RENAME COLUMN "Alarm"  TO alarm;
ALTER TABLE IF EXISTS alarms RENAME COLUMN "Date"   TO date;

-- orders ------------------------------------------------------------------
ALTER TABLE IF EXISTS orders RENAME COLUMN "Id"     TO id;
ALTER TABLE IF EXISTS orders RENAME COLUMN "Symbol" TO symbol;
ALTER TABLE IF EXISTS orders RENAME COLUMN "Time"   TO time;
ALTER TABLE IF EXISTS orders RENAME COLUMN "Stop"   TO stop;
ALTER TABLE IF EXISTS orders RENAME COLUMN "Date"   TO date;
ALTER TABLE IF EXISTS orders RENAME COLUMN "Status" TO status;

-- livedata (persistent bulk-insert target for raw ticks) ------------------
ALTER TABLE IF EXISTS livedata RENAME COLUMN "Symbol" TO symbol;
ALTER TABLE IF EXISTS livedata RENAME COLUMN "Date"   TO date;
ALTER TABLE IF EXISTS livedata RENAME COLUMN "Time"   TO time;
ALTER TABLE IF EXISTS livedata RENAME COLUMN "Last"   TO last;
ALTER TABLE IF EXISTS livedata RENAME COLUMN "Volume" TO volume;

-- watchlist / watchlist_strategies / exit_requests ------------------------
-- Already lowercase in schema; no rename needed.

COMMIT;

-- =========================================================================
-- Optional cleanup: drop <sym>_volume_model tables. SessionStore now
-- holds the rvol baseline in memory and delete_all_tables_db_async no
-- longer targets them; existing rows are dead weight.
-- =========================================================================
DO $$
DECLARE t RECORD;
BEGIN
    FOR t IN
        SELECT tablename
          FROM pg_tables
         WHERE schemaname = 'public'
           AND tablename LIKE '%_volume_model'
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', t.tablename);
    END LOOP;
END $$;
