from __future__ import annotations

import argparse
import json
import subprocess
import sys
import unittest
from pathlib import Path

from scripts.query_technology_intelligence import (
    ROOT,
    TechnologyIntelligenceIndex,
    build_parser,
    record_matches,
)


def parse_args(*args: str) -> argparse.Namespace:
    return build_parser().parse_args(list(args))


class TechnologyIntelligenceQueryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.index = TechnologyIntelligenceIndex()
        cls.records = cls.index.records()

    def query(self, *args: str):
        parsed = parse_args(*args)
        return [record for record in self.records if record_matches(record, parsed)]

    def test_cross_file_joins_are_valid(self) -> None:
        self.assertEqual([], self.index.validate_joins())
        self.assertEqual(42, len(self.index.products))
        self.assertEqual(42, len(self.index.deployments))
        self.assertGreater(len(self.index.sources), 0)
        self.assertGreater(len(self.index.decisions), 0)
        self.assertGreater(len(self.index.open_source_projects), 0)

    def test_database_filter_uses_database_evidence_not_research_gap_text(self) -> None:
        records = self.query("--database", "postgresql")
        ids = {record["id"] for record in records}
        self.assertIn("odoo/inventory", ids)
        self.assertNotIn("erpnext/stock-and-warehouse", ids)
        self.assertTrue(all(record["kind"] == "product" for record in records))

    def test_api_filter_finds_verified_rest_products_and_api_decision(self) -> None:
        records = self.query("--api", "rest")
        ids = {record["id"] for record in records}
        self.assertIn("logiwa/logiwa-io", ids)
        self.assertIn("openboxes/openboxes", ids)
        self.assertIn("oracle/warehouse-management-cloud", ids)
        self.assertIn("rest-openapi", ids)

    def test_deployment_filter_finds_self_hosted_products(self) -> None:
        records = self.query("--deployment", "self-hosted")
        ids = {record["id"] for record in records}
        self.assertIn("erpnext/stock-and-warehouse", ids)
        self.assertIn("odoo/inventory", ids)
        self.assertIn("openboxes/openboxes", ids)

    def test_company_filter_includes_product_and_decisions_that_cite_it(self) -> None:
        records = self.query("--company", "Manhattan Associates")
        ids = {record["id"] for record in records}
        self.assertIn("manhattan-associates/activewarehouse", ids)
        self.assertIn("kubernetes-default", ids)

    def test_trial_decision_filter_traverses_to_open_source_evidence(self) -> None:
        records = self.query("--decision", "trial")
        ids = {record["id"] for record in records}
        self.assertIn("grpc-internal", ids)
        self.assertIn("vda-5050", ids)
        vda_records = [record for record in records if record["id"] == "vda-5050"]
        self.assertTrue(any(record["kind"] == "open-source" for record in vda_records))
        self.assertTrue(any(record["kind"] == "decision" for record in vda_records))

    def test_research_gap_filter_returns_only_products_with_gaps(self) -> None:
        records = self.query("--research-gaps", "--verification", "research-gap")
        self.assertGreater(len(records), 0)
        self.assertTrue(all(record["kind"] == "product" for record in records))
        self.assertTrue(all(record["research_gaps"] for record in records))
        self.assertTrue(all(record["verification"]["state"] == "research-gap" for record in records))

    def test_json_cli_is_machine_readable_and_resolves_sources(self) -> None:
        command = [
            sys.executable,
            str(ROOT / "scripts" / "query_technology_intelligence.py"),
            "--company",
            "odoo",
            "--kind",
            "product",
            "--format",
            "json",
            "--validate",
        ]
        result = subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
        payload = json.loads(result.stdout)
        self.assertEqual(1, payload["summary"]["matched"])
        record = payload["records"][0]
        self.assertEqual("odoo/inventory", record["id"])
        self.assertEqual(2, record["evidence"]["source_count"])
        self.assertTrue(all(source["url"].startswith("http") for source in record["evidence"]["sources"]))


if __name__ == "__main__":
    unittest.main()
