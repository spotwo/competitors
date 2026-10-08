from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import yaml

from scripts.scaffold_event_pipeline import (
    DEFAULT_REGISTRY,
    ScaffoldRequest,
    _registry_yaml,
    handler_stub,
    scaffold,
    test_stub as generated_test_stub,
)
from event_pipeline_handler_loader import load_pipeline_handler_type
from event_pipeline_readiness import EventPipelineDeploymentRegistry
from event_pipeline_registry_validation import validate_global_consumer_identities
from event_pipeline_topology_config import DeploymentTopologyConfig


class EventPipelineScaffolderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.registry = self.root / "registry.yml"
        shutil.copyfile(DEFAULT_REGISTRY, self.registry)
        self.handlers = self.root / "handlers"
        self.tests = self.root / "tests"
        self.handlers.mkdir()
        self.tests.mkdir()
        self.request = ScaffoldRequest(
            pipeline_id="order-picked-projection",
            event_type="order.picked",
        )

    def run_scaffold(self, *, write: bool):
        return scaffold(
            self.request,
            registry_path=self.registry,
            handlers_dir=self.handlers,
            tests_dir=self.tests,
            write=write,
        )

    def test_dry_run_is_deterministic_and_mutates_nothing(self):
        before = self.registry.read_bytes()
        first, fragment = self.run_scaffold(write=False)
        second, second_fragment = self.run_scaffold(write=False)

        self.assertEqual(before, self.registry.read_bytes())
        self.assertEqual(first, second)
        self.assertEqual(fragment, second_fragment)
        self.assertEqual(list(self.handlers.iterdir()), [])
        self.assertEqual(list(self.tests.iterdir()), [])
        self.assertFalse(first["enabled"])
        self.assertEqual(
            first["topology"]["business_consumer"]["filter_subject"],
            "spotwo.wms.events.order.picked",
        )
        self.assertEqual(
            first["consumer"]["handler_module"],
            "event_pipeline_handlers.order_picked_projection",
        )
        self.assertEqual(
            first["consumer"]["projection_gap_monitor"], "none"
        )
        self.assertIn("  - id: order-picked-projection\n", fragment)

    def test_write_keeps_original_registry_comments_and_validates_all_pipelines(self):
        before = self.registry.read_text(encoding="utf-8")
        created, _ = self.run_scaffold(write=True)
        after = self.registry.read_text(encoding="utf-8")
        parsed = _registry_yaml(after)
        specs = EventPipelineDeploymentRegistry.from_mapping(parsed)

        self.assertTrue(after.startswith(before.rstrip("\n")))
        self.assertIn("# Strict ordering rejects gaps", after)
        self.assertEqual(len(specs.pipelines), len(_registry_yaml(before)['pipelines']) + 1)
        self.assertFalse(specs.get(created["id"]).enabled)
        validate_global_consumer_identities(specs)
        for item in parsed["pipelines"]:
            DeploymentTopologyConfig.from_mapping(item["topology"]).validate(
                specs.get(item["id"])
            )

        generated_handler = self.handlers / "order_picked_projection.py"
        generated_test = self.tests / "test_order_picked_projection_scaffold.py"
        self.assertEqual(
            generated_handler.read_text(encoding="utf-8"),
            handler_stub(self.request),
        )
        self.assertEqual(
            generated_test.read_text(encoding="utf-8"),
            generated_test_stub(self.request),
        )
        self.assertIn("NotImplementedError", generated_handler.read_text())
        self.assertIn("@pytest.mark.skip", generated_test.read_text())

        with self.assertRaisesRegex(ValueError, "already exists"):
            self.run_scaffold(write=True)
        self.assertEqual(after, self.registry.read_text(encoding="utf-8"))

    def test_duplicate_event_subject_is_rejected_without_writes(self):
        before = self.registry.read_bytes()
        self.request = ScaffoldRequest("duplicate-position", "inventory.position.changed")
        with self.assertRaisesRegex(ValueError, "business event subject already registered"):
            self.run_scaffold(write=True)
        self.assertEqual(before, self.registry.read_bytes())
        self.assertFalse(any(self.handlers.iterdir()))

    def test_preexisting_handler_file_is_not_overwritten(self):
        path = self.handlers / "order_picked_projection.py"
        path.write_text("preserve existing code", encoding="utf-8")
        before = self.registry.read_bytes()
        with self.assertRaisesRegex(ValueError, "refusing overwrite"):
            self.run_scaffold(write=True)
        self.assertEqual(before, self.registry.read_bytes())
        self.assertEqual(path.read_text(), "preserve existing code")
        self.assertFalse(any(self.tests.iterdir()))

    def test_rejects_invalid_identifiers_and_wildcards(self):
        for pipeline, event in [
            ("../../escape", "order.picked"),
            ("Uppercase", "order.picked"),
            ("ok-pipeline", "order.>"),
            ("ok-pipeline", "order.*"),
            ("ok-pipeline", "oneword"),
        ]:
            with self.subTest(pipeline=pipeline, event=event):
                with self.assertRaises(ValueError):
                    ScaffoldRequest(pipeline, event)

    def test_handler_loader_accepts_reviewed_builtins_only(self):
        source = _registry_yaml(self.registry.read_text(encoding="utf-8"))
        specs = EventPipelineDeploymentRegistry.from_mapping(source)
        # The knowledge gate intentionally has no psycopg dependency.
        # Mock the module boundary; real projector imports run in the PG/NATS lab.
        with mock.patch("event_pipeline_handler_loader.importlib.import_module") as importer:
            for spec in specs.pipelines:
                fake_handler = type(
                    spec.consumer.handler_class, (), {"__call__": lambda self, *_: None}
                )
                importer.return_value = SimpleNamespace(
                    **{spec.consumer.handler_class: fake_handler}
                )
                handler = load_pipeline_handler_type(spec.consumer)
                self.assertIs(handler, fake_handler)
                importer.assert_called_with(spec.consumer.handler_module)

        for module, name in [
            ("os", "PathLike"),
            ("subprocess", "Popen"),
            ("event_pipeline_handlers.__init__...garbage", "Bad"),
        ]:
            with self.subTest(module=module):
                with self.assertRaisesRegex(ValueError, "not allowlisted"):
                    load_pipeline_handler_type(
                        SimpleNamespace(handler_module=module, handler_class=name)
                    )

        with self.assertRaisesRegex(ValueError, "required"):
            load_pipeline_handler_type(
                SimpleNamespace(handler_module=None, handler_class=None)
            )

    def test_generated_handler_and_test_source_are_syntax_valid(self):
        compile(handler_stub(self.request), "<generated handler>", "exec")
        compile(generated_test_stub(self.request), "<generated test>", "exec")


if __name__ == "__main__":
    unittest.main()
