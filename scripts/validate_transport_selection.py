#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "kernel" / "transports.yml"
SCHEMA = ROOT / "schema" / "transport-selection.schema.json"


def main() -> int:
    errors: list[str] = []
    data = yaml.safe_load(MODEL.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    validator = Draft202012Validator(schema)
    for error in sorted(validator.iter_errors(data), key=lambda e: list(e.absolute_path)):
        path = ".".join(map(str, error.absolute_path)) or "<root>"
        errors.append(f"kernel/transports.yml: {path}: {error.message}")

    criteria = data.get("profile", {}).get("criteria", [])
    criterion_ids = [item.get("id") for item in criteria]
    if len(criterion_ids) != len(set(criterion_ids)):
        errors.append("kernel/transports.yml: duplicate criterion id")

    weights = {item["id"]: item["weight"] for item in criteria if item.get("id") and item.get("weight")}
    if sum(weights.values()) != 100:
        errors.append(f"kernel/transports.yml: criterion weights must total 100, got {sum(weights.values())}")

    evidence_ids: set[str] = set()
    for path in ROOT.glob("evidence/*.yml"):
        record = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(record, dict) and record.get("id"):
            evidence_ids.add(record["id"])

    candidates = data.get("candidates", [])
    candidate_ids = [candidate.get("id") for candidate in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        errors.append("kernel/transports.yml: duplicate candidate id")

    expected_score_keys = set(weights)
    totals: dict[str, float] = {}
    canonical_dispositions = 0

    for candidate in candidates:
        candidate_id = candidate.get("id", "<unknown>")
        if candidate.get("disposition") == "canonical-default":
            canonical_dispositions += 1

        missing_evidence = sorted(set(candidate.get("evidence_refs", [])) - evidence_ids)
        for evidence_id in missing_evidence:
            errors.append(f"kernel/transports.yml: candidate {candidate_id!r} missing evidence {evidence_id!r}")

        score_keys = set((candidate.get("scores") or {}).keys())
        if score_keys != expected_score_keys:
            missing = sorted(expected_score_keys - score_keys)
            extra = sorted(score_keys - expected_score_keys)
            if missing:
                errors.append(f"kernel/transports.yml: candidate {candidate_id!r} missing scores {missing}")
            if extra:
                errors.append(f"kernel/transports.yml: candidate {candidate_id!r} has unknown scores {extra}")
            continue

        totals[candidate_id] = sum(
            weights[criterion_id] * candidate["scores"][criterion_id]
            for criterion_id in weights
        ) / 100.0

    if canonical_dispositions != 1:
        errors.append(f"kernel/transports.yml: expected exactly one canonical-default disposition, got {canonical_dispositions}")

    canonical = data.get("decision", {}).get("canonical_default")
    if canonical not in totals:
        errors.append(f"kernel/transports.yml: canonical_default {canonical!r} is not a scored candidate")
    elif totals:
        ranking = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
        winner_id, winner_score = ranking[0]
        if canonical != winner_id:
            errors.append(
                f"kernel/transports.yml: canonical_default {canonical!r} is not highest scoring candidate {winner_id!r}"
            )
        if len(ranking) > 1:
            margin = winner_score - ranking[1][1]
            required_margin = float(data["decision"].get("required_margin_over_runner_up", 0))
            if margin + 1e-9 < required_margin:
                errors.append(
                    f"kernel/transports.yml: winner margin {margin:.2f} is below required {required_margin:.2f}"
                )

        canonical_records = [candidate for candidate in candidates if candidate.get("disposition") == "canonical-default"]
        if canonical_records and canonical_records[0].get("id") != canonical:
            errors.append("kernel/transports.yml: canonical-default disposition and decision.canonical_default disagree")

    candidate_id_set = set(candidate_ids)
    for profile_name, target in (data.get("decision", {}).get("exceptions") or {}).items():
        if target not in candidate_id_set:
            errors.append(f"kernel/transports.yml: exception {profile_name!r} targets unknown candidate {target!r}")

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    ranking = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    print("Transport selection validation passed")
    for rank, (candidate_id, total) in enumerate(ranking, start=1):
        marker = " <- canonical" if candidate_id == canonical else ""
        print(f"{rank}. {candidate_id}: {total:.2f}/5{marker}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
