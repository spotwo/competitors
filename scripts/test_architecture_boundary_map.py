from __future__ import annotations

import argparse
import unittest

from scripts import query_architecture_boundaries as query


class ArchitectureBoundaryMapTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.boundaries = query.load()

    def args(self, **overrides):
        values = {
            "id": None,
            "classification": None,
            "status": None,
            "technology": None,
            "standard": None,
            "gap": None,
            "has_gaps": False,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_initial_map_has_expected_contract_statuses(self):
        counts = {"canonical": 0, "candidate": 0, "unresolved": 0}
        for boundary in self.boundaries:
            counts[boundary["contract"]["status"]] += 1
        self.assertEqual(10, len(self.boundaries))
        self.assertEqual({"canonical": 2, "candidate": 6, "unresolved": 2}, counts)

    def test_canonical_contracts_are_rest_and_nats(self):
        canonical = {
            item["id"]: item["contract"]["technology_decision_ref"]
            for item in self.boundaries
            if item["contract"]["status"] == "canonical"
        }
        self.assertEqual(
            {
                "external-business-api": "rest-openapi",
                "internal-domain-event-backbone": "nats-jetstream",
            },
            canonical,
        )

    def test_unresolved_contracts_do_not_guess_primary_technology(self):
        unresolved = [item for item in self.boundaries if item["contract"]["status"] == "unresolved"]
        self.assertTrue(unresolved)
        self.assertTrue(all(item["contract"]["technology_decision_ref"] is None for item in unresolved))

    def test_technology_filter_uses_contract_and_evidence_not_research_gap_prose(self):
        matches = [item["id"] for item in self.boundaries if query.matches(item, self.args(technology="sparkplug"))]
        self.assertEqual(["device-telemetry"], matches)

    def test_standard_filter_finds_robot_boundary(self):
        matches = [item["id"] for item in self.boundaries if query.matches(item, self.args(standard="VDA 5050"))]
        self.assertEqual(["robot-fleet-control"], matches)

    def test_gap_query_surfaces_edge_contract_work(self):
        matches = [item["id"] for item in self.boundaries if query.matches(item, self.args(gap="enrollment"))]
        self.assertEqual(["edge-control-and-sync"], matches)


if __name__ == "__main__":
    unittest.main()
