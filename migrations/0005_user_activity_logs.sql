CREATE TABLE IF NOT EXISTS user_activity_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  event_type TEXT NOT NULL,
  action TEXT NOT NULL,
  anon_session_id TEXT,
  auth_slug TEXT,
  auth_display_name TEXT,
  session_hash TEXT,
  device_id TEXT,
  device_id_hash TEXT,
  method TEXT NOT NULL,
  host TEXT,
  path TEXT NOT NULL,
  query_redacted TEXT,
  status INTEGER,
  duration_ms INTEGER,
  ip TEXT,
  ip_hash TEXT,
  true_client_ip TEXT,
  forwarded_for TEXT,
  cf_ray TEXT,
  cf_colo TEXT,
  cf_country TEXT,
  cf_region TEXT,
  cf_city TEXT,
  cf_timezone TEXT,
  cf_asn INTEGER,
  cf_as_organization TEXT,
  user_agent TEXT,
  accept_language TEXT,
  referer TEXT,
  origin TEXT,
  sec_ch_ua TEXT,
  sec_ch_ua_mobile TEXT,
  sec_ch_ua_platform TEXT,
  sec_fetch_site TEXT,
  sec_fetch_mode TEXT,
  sec_fetch_dest TEXT,
  content_type TEXT,
  content_length INTEGER,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_user_activity_logs_created_at
  ON user_activity_logs(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_activity_logs_auth_created
  ON user_activity_logs(auth_slug, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_activity_logs_anon_created
  ON user_activity_logs(anon_session_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_activity_logs_ip_created
  ON user_activity_logs(ip, created_at DESC);
