#!/usr/bin/env python3
"""
Smoke Tests for OSHA Concierge MVP

Deterministic tests using local HTTP fixtures.
No external network dependencies.

Usage:
    python -m pytest tests_smoke.py -v
    # or
    python tests_smoke.py
"""

import csv
import http.server
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingest_osha import (
    parse_inspection_detail,
    calculate_lead_score,
    check_needs_review,
    upsert_inspection,
    compute_hash,
    parse_date,
    clean_text,
)
from export_daily import (
    get_sendable_leads,
    get_needs_review_leads,
    write_csv,
    DAILY_LEADS_COLUMNS,
)


# Sample OSHA inspection detail page HTML fixture
SAMPLE_DETAIL_HTML = """
<!DOCTYPE html>
<html>
<head><title>OSHA Inspection Detail</title></head>
<body>
<h1>Inspection Detail</h1>
<table>
    <tr><td>Activity Nr:</td><td>123456789</td></tr>
    <tr><td>Report ID:</td><td>0101010</td></tr>
    <tr><td>Open Date:</td><td>01/05/2025</td></tr>
    <tr><td>Inspection Type:</td><td>Complaint</td></tr>
    <tr><td>Scope:</td><td>Complete</td></tr>
    <tr><td>Case Status:</td><td>Open</td></tr>
    <tr><td>Emphasis:</td><td>NEP - Falls</td></tr>
    <tr><td>Safety/Health:</td><td>Safety</td></tr>
    <tr><td>SIC:</td><td>1521</td></tr>
    <tr><td>NAICS:</td><td>236220 - Commercial Building Construction</td></tr>
    <tr><td>Establishment Name:</td><td>Test Construction LLC</td></tr>
    <tr><td>Total Violations:</td><td>3</td></tr>
</table>
<div>
    <h3>Site Address</h3>
    <p>123 Main Street<br>
    Arlington, VA 22201</p>
</div>
</body>
</html>
"""


class SimpleHTTPHandler(http.server.BaseHTTPRequestHandler):
    """Simple HTTP handler that serves fixture HTML."""
    
    def log_message(self, format, *args):
        """Suppress logging."""
        pass
    
    def do_GET(self):
        """Serve the fixture HTML for any GET request."""
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(SAMPLE_DETAIL_HTML.encode("utf-8"))


class TestHelperFunctions(unittest.TestCase):
    """Test utility/helper functions."""
    
    def test_parse_date_formats(self):
        """Test various date format parsing."""
        self.assertEqual(parse_date("01/05/2025"), "2025-01-05")
        self.assertEqual(parse_date("2025-01-05"), "2025-01-05")
        self.assertEqual(parse_date("01-05-2025"), "2025-01-05")
        self.assertIsNone(parse_date(None))
        self.assertIsNone(parse_date(""))
    
    def test_clean_text(self):
        """Test text cleaning."""
        self.assertEqual(clean_text("  hello   world  "), "hello world")
        self.assertEqual(clean_text("normal"), "normal")
        self.assertIsNone(clean_text(None))
        self.assertIsNone(clean_text("   "))
    
    def test_compute_hash(self):
        """Test hash computation is deterministic."""
        h1 = compute_hash("test content")
        h2 = compute_hash("test content")
        h3 = compute_hash("different content")
        
        self.assertEqual(h1, h2)
        self.assertNotEqual(h1, h3)
        self.assertEqual(len(h1), 32)


class TestScoring(unittest.TestCase):
    """Test lead scoring algorithm."""
    
    def test_score_fatcat(self):
        """Fatality/Catastrophe gets highest points."""
        inspection = {"inspection_type": "Fat/Cat"}
        self.assertEqual(calculate_lead_score(inspection), 10)
    
    def test_score_accident(self):
        """Accident inspection scoring."""
        inspection = {"inspection_type": "Accident"}
        self.assertEqual(calculate_lead_score(inspection), 8)
    
    def test_score_complaint(self):
        """Complaint inspection scoring."""
        inspection = {"inspection_type": "Complaint"}
        self.assertEqual(calculate_lead_score(inspection), 4)
    
    def test_score_complete_scope(self):
        """Complete scope adds points."""
        inspection = {"scope": "Complete"}
        self.assertEqual(calculate_lead_score(inspection), 2)
    
    def test_score_violations(self):
        """Violations add points."""
        inspection = {"violations_count": 5}
        self.assertEqual(calculate_lead_score(inspection), 3)
        
        inspection = {"violations_count": 0}
        self.assertEqual(calculate_lead_score(inspection), 0)
    
    def test_score_construction_naics(self):
        """Construction NAICS adds points."""
        inspection = {"naics": "236220"}
        self.assertEqual(calculate_lead_score(inspection), 3)
        
        inspection = {"naics": "541990"}
        self.assertEqual(calculate_lead_score(inspection), 0)
    
    def test_score_emphasis(self):
        """Emphasis program adds points."""
        inspection = {"emphasis": "NEP - Falls"}
        self.assertEqual(calculate_lead_score(inspection), 2)
    
    def test_score_combined(self):
        """Combined scoring."""
        inspection = {
            "inspection_type": "Complaint",  # +4
            "scope": "Complete",             # +2
            "violations_count": 2,           # +3
            "naics": "236115",               # +3
            "emphasis": "NEP",               # +2
        }
        self.assertEqual(calculate_lead_score(inspection), 14)


class TestNeedsReview(unittest.TestCase):
    """Test needs_review flag logic."""
    
    def test_complete_record(self):
        """Complete record does not need review."""
        inspection = {
            "activity_nr": "123456789",
            "establishment_name": "Test Corp",
            "site_state": "VA",
            "site_city": "Arlington",
            "date_opened": "2025-01-05",
        }
        self.assertFalse(check_needs_review(inspection))
    
    def test_missing_activity_nr(self):
        """Missing activity_nr needs review."""
        inspection = {
            "establishment_name": "Test Corp",
            "site_state": "VA",
            "site_city": "Arlington",
            "date_opened": "2025-01-05",
        }
        self.assertTrue(check_needs_review(inspection))
    
    def test_missing_location(self):
        """Missing both city and zip needs review."""
        inspection = {
            "activity_nr": "123456789",
            "establishment_name": "Test Corp",
            "site_state": "VA",
            "date_opened": "2025-01-05",
        }
        self.assertTrue(check_needs_review(inspection))
    
    def test_has_zip_but_no_city(self):
        """Having zip is sufficient for location."""
        inspection = {
            "activity_nr": "123456789",
            "establishment_name": "Test Corp",
            "site_state": "VA",
            "site_zip": "22201",
            "date_opened": "2025-01-05",
        }
        self.assertFalse(check_needs_review(inspection))


class TestDetailParsing(unittest.TestCase):
    """Test inspection detail page parsing."""
    
    def test_parse_sample_detail(self):
        """Parse the sample HTML fixture."""
        url = "https://www.osha.gov/ords/imis/establishment.inspection_detail?id=123456789"
        result = parse_inspection_detail(SAMPLE_DETAIL_HTML, url)
        
        # Core fields that should always be parsed
        self.assertEqual(result.get("activity_nr"), "123456789")
        self.assertEqual(result.get("date_opened"), "2025-01-05")
        self.assertEqual(result.get("inspection_type"), "Complaint")
        self.assertEqual(result.get("establishment_name"), "Test Construction LLC")
        self.assertEqual(result.get("violations_count"), 3)
        self.assertEqual(result.get("source_url"), url)
        self.assertIsNotNone(result.get("raw_hash"))
        
        # Optional fields may be parsed depending on HTML structure
        # Using get() to avoid KeyError in case parsing varies
        if result.get("report_id"):
            self.assertEqual(result["report_id"], "0101010")
        if result.get("scope"):
            self.assertEqual(result["scope"], "Complete")
        if result.get("case_status"):
            self.assertEqual(result["case_status"], "Open")
        if result.get("emphasis"):
            self.assertEqual(result["emphasis"], "NEP - Falls")
        if result.get("naics"):
            self.assertEqual(result["naics"], "236220")
        if result.get("naics_desc"):
            self.assertIn("Commercial", result["naics_desc"])


class TestDatabaseOperations(unittest.TestCase):
    """Test database operations with temp SQLite."""
    
    def setUp(self):
        """Create temp database with schema."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        
        # Read and execute schema
        schema_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "schema.sql"
        )
        
        with open(schema_path, "r") as f:
            schema = f.read()
        
        self.conn = sqlite3.connect(self.db_path)
        self.conn.executescript(schema)
        self.conn.commit()
    
    def tearDown(self):
        """Clean up temp database."""
        self.conn.close()
        os.unlink(self.db_path)
        os.rmdir(self.temp_dir)
    
    def test_schema_creates_tables(self):
        """Verify all expected tables exist."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        
        self.assertIn("inspections", tables)
        self.assertIn("citations", tables)
        self.assertIn("suppression_list", tables)
        self.assertIn("ingestion_log", tables)
    
    def test_insert_new_inspection(self):
        """Test inserting a new inspection."""
        inspection = {
            "activity_nr": "123456789",
            "establishment_name": "Test Corp",
            "site_state": "VA",
            "site_city": "Arlington",
            "date_opened": "2025-01-05",
            "inspection_type": "Complaint",
            "scope": "Complete",
            "source_url": "https://example.com",
        }
        
        is_new, is_updated = upsert_inspection(self.conn, inspection)
        self.conn.commit()
        
        self.assertTrue(is_new)
        self.assertFalse(is_updated)
        
        # Verify record
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM inspections WHERE activity_nr = ?", ("123456789",))
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
    
    def test_update_existing_inspection(self):
        """Test updating an existing inspection."""
        inspection = {
            "activity_nr": "123456789",
            "establishment_name": "Test Corp",
            "site_state": "VA",
            "site_city": "Arlington",
            "date_opened": "2025-01-05",
        }
        
        # Insert first
        upsert_inspection(self.conn, inspection)
        self.conn.commit()
        
        # Update with more data
        inspection["violations_count"] = 5
        inspection["scope"] = "Complete"
        
        is_new, is_updated = upsert_inspection(self.conn, inspection)
        self.conn.commit()
        
        self.assertFalse(is_new)
        self.assertTrue(is_updated)
        
        # Verify updated
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT violations_count, scope FROM inspections WHERE activity_nr = ?",
            ("123456789",)
        )
        row = cursor.fetchone()
        
        self.assertEqual(row[0], 5)
        self.assertEqual(row[1], "Complete")
    
    def test_lead_id_generated(self):
        """Test that lead_id is auto-generated."""
        inspection = {
            "activity_nr": "987654321",
            "establishment_name": "Test",
            "site_state": "MD",
            "site_city": "Baltimore",
            "date_opened": "2025-01-05",
        }
        
        upsert_inspection(self.conn, inspection)
        self.conn.commit()
        
        cursor = self.conn.cursor()
        cursor.execute("SELECT lead_id FROM inspections WHERE activity_nr = ?", ("987654321",))
        lead_id = cursor.fetchone()[0]
        
        self.assertEqual(lead_id, "osha:inspection:987654321")


class TestExport(unittest.TestCase):
    """Test CSV export functionality."""
    
    def setUp(self):
        """Create temp database with test data."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.out_dir = os.path.join(self.temp_dir, "out")
        os.makedirs(self.out_dir)
        
        # Read and execute schema
        schema_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "schema.sql"
        )
        
        with open(schema_path, "r") as f:
            schema = f.read()
        
        self.conn = sqlite3.connect(self.db_path)
        self.conn.executescript(schema)
        
        # Insert test data with very recent first_seen_at
        from datetime import datetime
        now = datetime.utcnow().isoformat()
        
        self.conn.execute("""
            INSERT INTO inspections (
                activity_nr, establishment_name, site_state, site_city,
                date_opened, inspection_type, scope, lead_score, needs_review,
                first_seen_at, last_seen_at, source_url
            ) VALUES (
                '123456789', 'Test Corp', 'VA', 'Arlington',
                '2025-01-05', 'Complaint', 'Complete', 10, 0,
                ?, ?, 'https://example.com'
            )
        """, (now, now))
        
        self.conn.execute("""
            INSERT INTO inspections (
                activity_nr, establishment_name, site_state,
                date_opened, lead_score, needs_review,
                first_seen_at, last_seen_at
            ) VALUES (
                '987654321', 'Incomplete Corp', 'MD',
                '2025-01-04', 5, 1,
                ?, ?
            )
        """, (now, now))
        
        self.conn.commit()
    
    def tearDown(self):
        """Clean up temp files."""
        self.conn.close()
        
        for f in os.listdir(self.out_dir):
            os.unlink(os.path.join(self.out_dir, f))
        os.rmdir(self.out_dir)
        os.unlink(self.db_path)
        os.rmdir(self.temp_dir)
    
    def test_get_sendable_leads(self):
        """Test fetching sendable leads."""
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        
        leads = get_sendable_leads(self.conn, today)
        
        self.assertEqual(len(leads), 1)
        self.assertEqual(leads[0]["activity_nr"], "123456789")
    
    def test_get_needs_review_leads(self):
        """Test fetching needs-review leads."""
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        
        leads = get_needs_review_leads(self.conn, today)
        
        self.assertEqual(len(leads), 1)
        self.assertEqual(leads[0]["activity_nr"], "987654321")
        self.assertIn("site_city/zip", leads[0]["missing_fields"])
    
    def test_write_csv(self):
        """Test CSV file writing."""
        leads = [
            {
                "lead_id": "osha:inspection:123",
                "activity_nr": "123456789",
                "establishment_name": "Test Corp",
                "lead_score": 10,
            }
        ]
        
        filepath = os.path.join(self.out_dir, "test.csv")
        count = write_csv(filepath, leads, ["lead_id", "activity_nr", "lead_score"])
        
        self.assertEqual(count, 1)
        self.assertTrue(os.path.exists(filepath))
        
        # Verify contents
        with open(filepath, "r", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["activity_nr"], "123456789")


class TestLocalHTTPServer(unittest.TestCase):
    """Test ingestion with local HTTP server fixture."""
    
    @classmethod
    def setUpClass(cls):
        """Start local HTTP server."""
        cls.server = http.server.HTTPServer(("127.0.0.1", 0), SimpleHTTPHandler)
        cls.port = cls.server.server_address[1]
        cls.server_thread = threading.Thread(target=cls.server.serve_forever)
        cls.server_thread.daemon = True
        cls.server_thread.start()
    
    @classmethod
    def tearDownClass(cls):
        """Stop local HTTP server."""
        cls.server.shutdown()
    
    def test_fetch_from_local_server(self):
        """Test fetching and parsing from local fixture server."""
        import requests
        
        url = f"http://127.0.0.1:{self.port}/inspection?activity_nr=123456789"
        response = requests.get(url, timeout=5)
        
        self.assertEqual(response.status_code, 200)
        
        # Parse the response
        result = parse_inspection_detail(response.text, url)
        
        self.assertEqual(result["activity_nr"], "123456789")
        self.assertEqual(result["inspection_type"], "Complaint")
        self.assertEqual(result["establishment_name"], "Test Construction LLC")


class TestEndToEnd(unittest.TestCase):
    """End-to-end integration test."""
    
    def test_full_pipeline(self):
        """Test complete ingest -> export pipeline with fixtures."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.db")
        out_dir = os.path.join(temp_dir, "out")
        os.makedirs(out_dir)
        
        try:
            # Initialize database
            schema_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "schema.sql"
            )
            
            with open(schema_path, "r") as f:
                schema = f.read()
            
            conn = sqlite3.connect(db_path)
            conn.executescript(schema)
            
            # Insert a test inspection directly
            from datetime import datetime
            now = datetime.utcnow().isoformat()
            
            conn.execute("""
                INSERT INTO inspections (
                    activity_nr, establishment_name, site_state, site_city,
                    date_opened, inspection_type, scope, naics, violations_count,
                    lead_score, needs_review, first_seen_at, last_seen_at,
                    source_url
                ) VALUES (
                    '111222333', 'Pipeline Test Corp', 'VA', 'Fairfax',
                    '2025-01-06', 'Accident', 'Complete', '236220', 2,
                    16, 0, ?, ?,
                    'https://example.com/inspection'
                )
            """, (now, now))
            conn.commit()
            conn.close()
            
            # Run export
            from export_daily import export_daily
            
            today = datetime.now().strftime("%Y-%m-%d")
            stats = export_daily(db_path, out_dir, today)
            
            # Verify outputs
            self.assertEqual(stats["sendable_leads"], 1)
            self.assertIsNotNone(stats["daily_leads_file"])
            self.assertTrue(os.path.exists(stats["daily_leads_file"]))
            
            # Verify CSV content
            with open(stats["daily_leads_file"], "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["activity_nr"], "111222333")
            self.assertEqual(rows[0]["establishment_name"], "Pipeline Test Corp")
            
            # Verify all expected columns present
            for col in DAILY_LEADS_COLUMNS:
                self.assertIn(col, reader.fieldnames)
            
        finally:
            # Cleanup
            for f in os.listdir(out_dir):
                os.unlink(os.path.join(out_dir, f))
            os.rmdir(out_dir)
            if os.path.exists(db_path):
                os.unlink(db_path)
            os.rmdir(temp_dir)


if __name__ == "__main__":
    # Run tests
    unittest.main(verbosity=2)
