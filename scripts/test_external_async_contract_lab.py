#!/usr/bin/env python3
from __future__ import annotations

import unittest

from scripts.validate_external_async_contract_lab import classify_http_status, signature, validate, verify_signature


class ExternalAsyncContractLabTest(unittest.TestCase):
    def test_lab_contract_is_valid(self):
        self.assertEqual([], validate())

    def test_signature_binds_id_timestamp_and_raw_body(self):
        secret = b"secret"
        body = b'{"ok":true}'
        message_id = "evt_01"
        timestamp = "1787161200"
        supplied = signature(secret, message_id, timestamp, body)

        self.assertTrue(verify_signature(secret, message_id, timestamp, body, supplied))
        self.assertFalse(verify_signature(secret, "evt_02", timestamp, body, supplied))
        self.assertFalse(verify_signature(secret, message_id, "1787161201", body, supplied))
        self.assertFalse(verify_signature(secret, message_id, timestamp, body + b" ", supplied))

    def test_retry_classifier_is_duplicate_capable(self):
        self.assertEqual("success", classify_http_status(200))
        self.assertEqual("success", classify_http_status(202))
        self.assertEqual("retry", classify_http_status(408))
        self.assertEqual("retry", classify_http_status(429))
        self.assertEqual("retry", classify_http_status(503))
        self.assertEqual("terminal", classify_http_status(400))
        self.assertEqual("terminal", classify_http_status(404))

    def test_unexpected_status_is_rejected(self):
        with self.assertRaises(ValueError):
            classify_http_status(302)


if __name__ == "__main__":
    unittest.main()
