import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from outreach import crm_store
from outreach import run_prospect_generation as generator


class TestProspectGenerationBacklog(unittest.TestCase):
    def test_compute_uncontacted_backlog_filters_correctly(self):
        with tempfile.TemporaryDirectory() as d:
            data_dir = Path(d) / "data"
            db_path = data_dir / "crm.sqlite"
            crm_store.ensure_database(path=db_path)

            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            conn = crm_store.connect(db_path)
            try:
                # Eligible
                conn.execute(
                    """
                    INSERT INTO prospects(
                      prospect_id, firm, contact_name, email, title, city, state, website, source,
                      score, status, created_at, last_contacted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("p_ok", "Firm", "", "ok@business.com", "Owner", "Austin", "TX", "", "seed", 0, "new", now, None),
                )
                # Suppressed
                conn.execute(
                    "INSERT INTO prospects(prospect_id,firm,contact_name,email,title,city,state,website,source,score,status,created_at,last_contacted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("p_supp", "Firm", "", "supp@business.com", "Owner", "Austin", "TX", "", "seed", 0, "new", now, None),
                )
                # Excluded status
                conn.execute(
                    "INSERT INTO prospects(prospect_id,firm,contact_name,email,title,city,state,website,source,score,status,created_at,last_contacted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("p_dnc", "Firm", "", "dnc@business.com", "Owner", "Austin", "TX", "", "seed", 0, "do_not_contact", now, None),
                )
                # Already contacted via event
                conn.execute(
                    "INSERT INTO prospects(prospect_id,firm,contact_name,email,title,city,state,website,source,score,status,created_at,last_contacted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("p_sent", "Firm", "", "sent@business.com", "Owner", "Austin", "TX", "", "seed", 0, "new", now, None),
                )
                conn.execute(
                    "INSERT INTO outreach_events(prospect_id, ts, event_type, batch_id, metadata_json) VALUES (?, ?, 'sent', ?, '{}')",
                    ("p_sent", now, "2026-02-18_TX"),
                )
                # already_contacted via last_contacted_at
                conn.execute(
                    "INSERT INTO prospects(prospect_id,firm,contact_name,email,title,city,state,website,source,score,status,created_at,last_contacted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("p_last", "Firm", "", "last@business.com", "Owner", "Austin", "TX", "", "seed", 0, "new", now, now),
                )
                # invalid shape
                conn.execute(
                    "INSERT INTO prospects(prospect_id,firm,contact_name,email,title,city,state,website,source,score,status,created_at,last_contacted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("p_bad", "Firm", "", "bad-email", "Owner", "Austin", "TX", "", "seed", 0, "new", now, None),
                )
                # role inbox
                conn.execute(
                    "INSERT INTO prospects(prospect_id,firm,contact_name,email,title,city,state,website,source,score,status,created_at,last_contacted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("p_role", "Firm", "", "info@business.com", "Owner", "Austin", "TX", "", "seed", 0, "new", now, None),
                )
                # free domain
                conn.execute(
                    "INSERT INTO prospects(prospect_id,firm,contact_name,email,title,city,state,website,source,score,status,created_at,last_contacted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("p_free", "Firm", "", "free@gmail.com", "Owner", "Austin", "TX", "", "seed", 0, "new", now, None),
                )
                conn.commit()

                suppressed = {"supp@business.com"}
                count = generator.compute_uncontacted_backlog(
                    conn=conn,
                    state="TX",
                    suppressed_emails=suppressed,
                    skip_role_inboxes=True,
                )
                self.assertEqual(count, 2)
                count_with_role = generator.compute_uncontacted_backlog(
                    conn=conn,
                    state="TX",
                    suppressed_emails=suppressed,
                    skip_role_inboxes=False,
                )
                self.assertEqual(count_with_role, 2)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
