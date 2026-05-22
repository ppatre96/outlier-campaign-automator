-- 007_slack_thread_ts.sql
--
-- One Slack thread per ramp (2026-05-22). The first notification we send for
-- a ramp (notify_new_ramp) gets the channel `ts` written here; subsequent
-- notifications (briefs_ready, launched) read this column and post with
-- `thread_ts` so they appear as replies in the SAME thread.
--
-- Apply with:
--   doppler run -p outlier-campaign-agent -c dev -- bash -c \
--     'psql "$DATABASE_URL" -f scripts/sql/007_slack_thread_ts.sql'
--
-- Idempotent — ADD COLUMN IF NOT EXISTS.

ALTER TABLE ramp_decisions ADD COLUMN IF NOT EXISTS slack_thread_ts TEXT;
