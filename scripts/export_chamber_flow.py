"""Write a session's replay results as chamber_flow records for other repos.

    python scripts/export_chamber_flow.py 2025R1
    python scripts/export_chamber_flow.py 2025R1 --out /some/dir/2025R1.chamber_flow.jsonl

Other CLS-1000 components (the Notitia Civica newsroom first) read proc_track's
results as data instead of importing replay_harness.py. This script replays
every measure in cache/<session>.json, exactly as bulk_replay.py does, and
writes one record per measure to exports/<session>.chamber_flow.jsonl.

Each line is an Ecosystem Exchange v1 envelope (producer "proc_track",
record_type "chamber_flow"), written with the stdlib to the JSON Schema in
CLS-1000/notitia-civica-newsroom schemas/exchange-v1.schema.json. The payload
holds everything a consumer needs to re-derive a lead without running this
repo's code:

- completed / halt: whether the replay finished, and the halt message if not;
- final_states, replay_payload, history: the replay's own result;
- rows: the measure's rows after apply_corrections, in ActionDate order, each
  paired (row_rule_ids, same index) with the rule_ids parse_row emitted for it;
- cached_rows: the rows exactly as the session cache holds them, for sealing.

Every record carries the proc_track commit, the sha256 of replay_harness.py
and of the session cache, so a consumer can tell which rules produced it.

Nothing is fetched. A halted measure is exported too, with completed=false: a
halt is a rule gap, and the consumer decides what to do with it.
"""
import argparse
import collections
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import replay_harness as h  # noqa: E402

CACHE = os.path.join(ROOT, "cache")
EXPORTS = os.path.join(ROOT, "exports")
SCHEMA_VERSION = "1"
PRODUCER = "proc_track"
RECORD_TYPE = "chamber_flow"


def sha256_file(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def git_commit():
    try:
        out = subprocess.run(["git", "-C", ROOT, "rev-parse", "HEAD"],
                             capture_output=True, text=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        return None
    return out.stdout.strip() or None


def utc_stamp(moment):
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def session_rows(session):
    path = os.path.join(CACHE, f"{session}.json")
    if not os.path.exists(path):
        sys.exit(f"no cache for {session} — run: python scripts/fetch_session.py {session}")
    with open(path, encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        sys.exit(f"{path} is not a list of history rows")
    by = collections.defaultdict(list)
    other_session = 0
    for r in rows:
        if not isinstance(r, dict) or r.get("SessionKey") != session:
            other_session += 1
            continue
        by[f"{r.get('MeasurePrefix', '')}{r.get('MeasureNumber', '')}"].append(r)
    return dict(by), sha256_file(path), other_session


def order(rows):
    # ActionDate is the only defensible sort key; MeasureHistoryId contradicts it
    return sorted(rows, key=lambda r: (r.get("ActionDate") or "", r.get("MeasureHistoryId") or 0))


def measure_record(session, measure, cached, stamps):
    ordered = order(h.apply_corrections(cached))
    item, halt, _gaps = h.replay(measure, ordered)
    row_rule_ids = [[e.rule_id for e in h.parse_row(r.get("ActionText") or "")[0]]
                    for r in ordered]
    return {
        "session": session,
        "measure": measure,
        "measure_prefix": str(cached[0].get("MeasurePrefix", "")),
        "measure_number": cached[0].get("MeasureNumber"),
        "completed": halt is None,
        "halt": halt,
        "final_states": dict(item.states),
        "replay_payload": item.payload,
        "history": [
            {"occurred_at": e[0], "chamber": e[1], "from_state": e[2],
             "to_state": e[3], "rule_id": e[4]}
            for e in item.history
        ],
        "rows": ordered,
        "row_rule_ids": row_rule_ids,
        "cached_rows": cached,
        **stamps,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("session")
    ap.add_argument("--out", help="output file (default exports/<session>.chamber_flow.jsonl)")
    args = ap.parse_args(argv)

    by, cache_sha256, other_session = session_rows(args.session)
    stamps = {
        "proc_track_commit": git_commit(),
        "harness_sha256": sha256_file(os.path.join(ROOT, "replay_harness.py")),
        "cache_sha256": cache_sha256,
    }
    created_at = utc_stamp(datetime.now(timezone.utc))
    out = args.out or os.path.join(EXPORTS, f"{args.session}.chamber_flow.jsonl")
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)

    completed = halted = 0
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(out)), suffix=".partial")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for measure in sorted(by):
                payload = measure_record(args.session, measure, by[measure], stamps)
                completed += payload["completed"]
                halted += not payload["completed"]
                envelope = {
                    "schema_version": SCHEMA_VERSION,
                    "producer": PRODUCER,
                    "record_type": RECORD_TYPE,
                    "record_id": f"{args.session}/{measure}",
                    "created_at": created_at,
                    "payload": payload,
                }
                f.write(json.dumps(envelope, ensure_ascii=False, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, out)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise

    print(f"{args.session}: {completed + halted} measures exported "
          f"({completed} completed, {halted} halted, {other_session} rows from other sessions skipped)")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
