#!/usr/bin/env python3
from __future__ import annotations

import argparse
import unittest

from scripts.query_architecture_coverage import matches, records


def args(**overrides):
    values = {
        "boundary": None,
        "priority": None,
        "overall": None,
        "contract_status": None,
        "classification": None,
        "lens": None,
        "lens_status": None,
        "target": None,
        "gap": None,
        "evidence": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class ArchitectureCoverageQueryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.records = records()

    def select(self, **filters):
        query = args(**filters)
        return [item for item in self.records if matches(item, query)]

    def test_every_boundary_is_queryable(self):
        self.assertEqual(10, len(self.records))
        self.assertEqual(10, len({item["boundary_ref"] for item in self.records}))

    def test_p0_queue_is_bounded(self):
        self.assertEqual(
            {
                "external-async-integration",
                "edge-control-and-sync",
                "plc-control",
                "robot-fleet-control",
            },
            {item["boundary_ref"] for item in self.select(priority="P0")},
        )

    def test_missing_coverage_identifies_external_async(self):
        self.assertEqual(
            ["external-async-integration"],
            [item["boundary_ref"] for item in self.select(overall="missing")],
        )

    def test_executable_missing_filter_finds_unproven_boundaries(self):
        result = self.select(lens="executable", lens_status="missing")
        self.assertEqual(8, len(result))
        self.assertNotIn("external-business-api", {item["boundary_ref"] for item in result})
        self.assertNotIn("internal-domain-event-backbone", {item["boundary_ref"] for item in result})

    def test_target_filter_searches_targets_not_gap_prose(self):
        result = self.select(target="open62541")
        self.assertEqual(
            {"device-telemetry", "plc-control"},
            {item["boundary_ref"] for item in result},
        )

    def test_gap_filter_searches_gaps_not_targets(self):
        result = self.select(gap="reconnect")
        self.assertEqual(
            {"device-telemetry", "plc-control", "robot-fleet-control"},
            {item["boundary_ref"] for item in result},
        )
        self.assertNotIn("edge-control-and-sync", {item["boundary_ref"] for item in result})
        self.assertEqual(
            ["edge-control-and-sync"],
            [item["boundary_ref"] for item in self.select(target="reconnect")],
        )

    def test_evidence_filter_uses_evidence_only(self):
        result = self.select(evidence="eclipse-ditto")
        self.assertEqual(
            ["edge-control-and-sync"],
            [item["boundary_ref"] for item in result],
        )

    def test_contract_status_filter_keeps_architecture_context(self):
        unresolved = self.select(contract_status="unresolved")
        self.assertEqual(
            {"external-async-integration", "edge-control-and-sync"},
            {item["boundary_ref"] for item in unresolved},
        )


if __name__ == "__main__":
    unittest.main()
