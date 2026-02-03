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
    
    -- Mailing address (optional)
    mail_address1 TEXT,
    mail_city TEXT,
    mail_state TEXT,
    mail_zip TEXT,
    
    -- Tracking
    report_id TEXT,
    source_url TEXT,
    raw_hash TEXT,
    
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
CREATE INDEX IF NOT EXISTS idx_citations_inspection_id ON citations(inspection_id);
CREATE INDEX IF NOT EXISTS idx_suppression_email ON suppression_list(email_or_domain);

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
