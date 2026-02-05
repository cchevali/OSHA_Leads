-- OSHA Concierge MVP Schema v0.1
-- SQLite database schema for inspection lead tracking

-- Enable foreign keys
PRAGMA foreign_keys = ON;

-- Main inspections table
CREATE TABLE IF NOT EXISTS inspections (
    -- Primary key
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Canonical identifiers
    activity_nr TEXT UNIQUE NOT NULL,
    lead_id TEXT GENERATED ALWAYS AS ('osha:inspection:' || activity_nr) STORED,
    
    -- Dates
    date_opened DATE,
    first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Inspection metadata
    inspection_type TEXT,
    scope TEXT,
    case_status TEXT,
    emphasis TEXT,
    safety_health TEXT,
    
    -- Industry codes
    sic TEXT,
    naics TEXT,
    naics_desc TEXT,
    
    -- Violation counts
    violations_count INTEGER,
    serious_violations INTEGER,
    willful_violations INTEGER,
    repeat_violations INTEGER,
    other_violations INTEGER,
    
    -- Establishment info
    establishment_name TEXT,
    
    -- Site address
    site_address1 TEXT,
    site_city TEXT,
    site_state TEXT,
    site_zip TEXT,
    area_office TEXT,
    
    -- Mailing address (optional)
    mail_address1 TEXT,
    mail_city TEXT,
    mail_state TEXT,
    mail_zip TEXT,
    
    -- Tracking
    report_id TEXT,
    source_url TEXT,
    raw_hash TEXT,
    record_hash TEXT,
    changed_at DATETIME,
    
    -- Status flags
    needs_review INTEGER NOT NULL DEFAULT 0,
    re_alert INTEGER NOT NULL DEFAULT 0,
    parse_invalid INTEGER NOT NULL DEFAULT 0,
    
    -- Scoring
    lead_score INTEGER NOT NULL DEFAULT 0,
    
    -- Audit
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Citations table (placeholder for future phase)
CREATE TABLE IF NOT EXISTS citations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inspection_id INTEGER NOT NULL,
    citation_id TEXT,
    citation_type TEXT,
    description TEXT,
    penalty_initial REAL,
    penalty_current REAL,
    abatement_date DATE,
    first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_url TEXT,
    raw_hash TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (inspection_id) REFERENCES inspections(id) ON DELETE CASCADE
);

-- Suppression list for opt-outs
CREATE TABLE IF NOT EXISTS suppression_list (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email_or_domain TEXT UNIQUE NOT NULL,
    reason TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Territory definitions used by subscriber filters
CREATE TABLE IF NOT EXISTS territories (
    territory_code TEXT PRIMARY KEY,
    description TEXT,
    states_json TEXT NOT NULL,
    office_patterns_json TEXT NOT NULL,
    fallback_city_patterns_json TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Subscriber registry for trial and recurring delivery
CREATE TABLE IF NOT EXISTS subscribers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subscriber_key TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    recipients_json TEXT,
    last_sent_at DATETIME,
    territory_code TEXT NOT NULL,
    content_filter TEXT NOT NULL DEFAULT 'high_medium',
    include_low_fallback INTEGER NOT NULL DEFAULT 0,
    trial_length_days INTEGER NOT NULL DEFAULT 14,
    trial_started_at DATE NOT NULL,
    trial_ends_at DATE,
    active INTEGER NOT NULL DEFAULT 1,
    send_enabled INTEGER NOT NULL DEFAULT 0,
    send_time_local TEXT NOT NULL DEFAULT '08:00',
    timezone TEXT NOT NULL DEFAULT 'America/Chicago',
    customer_id TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (territory_code) REFERENCES territories(territory_code)
);

-- Append-only unsubscribe/suppression events
CREATE TABLE IF NOT EXISTS unsubscribe_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL,
    event_type TEXT NOT NULL,
    reason TEXT,
    source TEXT NOT NULL,
    customer_id TEXT,
    territory_code TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Ingestion log for source tracking
CREATE TABLE IF NOT EXISTS ingestion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_started_at DATETIME NOT NULL,
    run_completed_at DATETIME,
    states_queried TEXT,
    since_days INTEGER,
    results_found INTEGER DEFAULT 0,
    details_fetched INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running',
    error_message TEXT
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_inspections_date_opened ON inspections(date_opened);
CREATE INDEX IF NOT EXISTS idx_inspections_site_state ON inspections(site_state);
CREATE INDEX IF NOT EXISTS idx_inspections_first_seen ON inspections(first_seen_at);
CREATE INDEX IF NOT EXISTS idx_inspections_needs_review ON inspections(needs_review);
CREATE INDEX IF NOT EXISTS idx_inspections_lead_score ON inspections(lead_score DESC);
CREATE INDEX IF NOT EXISTS idx_inspections_area_office ON inspections(area_office);
CREATE INDEX IF NOT EXISTS idx_citations_inspection_id ON citations(inspection_id);
CREATE INDEX IF NOT EXISTS idx_suppression_email ON suppression_list(email_or_domain);
CREATE INDEX IF NOT EXISTS idx_subscribers_active ON subscribers(active);
CREATE INDEX IF NOT EXISTS idx_subscribers_send_time ON subscribers(send_time_local, timezone);
CREATE INDEX IF NOT EXISTS idx_unsubscribe_events_email ON unsubscribe_events(email);

-- Trigger to update updated_at on inspections
CREATE TRIGGER IF NOT EXISTS update_inspections_timestamp 
AFTER UPDATE ON inspections
BEGIN
    UPDATE inspections SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Trigger to update updated_at on citations
CREATE TRIGGER IF NOT EXISTS update_citations_timestamp 
AFTER UPDATE ON citations
BEGIN
    UPDATE citations SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Trigger to update updated_at on territories
CREATE TRIGGER IF NOT EXISTS update_territories_timestamp
AFTER UPDATE ON territories
BEGIN
    UPDATE territories SET updated_at = CURRENT_TIMESTAMP WHERE territory_code = NEW.territory_code;
END;

-- Trigger to update updated_at on subscribers
CREATE TRIGGER IF NOT EXISTS update_subscribers_timestamp
AFTER UPDATE ON subscribers
BEGIN
    UPDATE subscribers SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;
