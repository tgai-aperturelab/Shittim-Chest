"""Single-owner SQLite records and conditional transitions for the candidate runtime."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import uuid
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from shittim_core.domain.types import (
    AccessContext,
    ControlReceipt,
    ConversationAddress,
    InputEvent,
    OutboundIntent,
    ProtocolReceipt,
    RunHalted,
    Segment,
    StaleView,
    StateConflict,
    ToolResult,
)

type Record = dict[str, Any]


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _id(kind: str, value: str) -> str:
    return f"{kind}_{uuid.uuid5(uuid.NAMESPACE_URL, kind + ':' + value).hex}"


def _access_dict(access: AccessContext) -> Record:
    return {
        "actor_id": access.actor_id,
        "audience": access.audience,
        "allowed_tools": sorted(access.allowed_tools),
        "allowed_resources": sorted(access.allowed_resources),
        "allowed_recipients": sorted(access.allowed_recipients),
        "policy_version": access.policy_version,
    }


def _access(value: str) -> AccessContext:
    data = json.loads(value)
    for key in ("allowed_tools", "allowed_resources", "allowed_recipients"):
        data[key] = frozenset(data[key])
    return AccessContext(**data)


def _event(value: str) -> InputEvent:
    data = json.loads(value)
    data["address"] = ConversationAddress(**data["address"])
    data["segments"] = tuple(Segment(**s) for s in data["segments"])
    return InputEvent(**data)


def _intent(value: str) -> OutboundIntent:
    data = json.loads(value)
    data["address"] = ConversationAddress(**data["address"])
    data["artifact_refs"] = tuple(data["artifact_refs"])
    data["forward_message_ids"] = tuple(data.get("forward_message_ids", ()))
    return OutboundIntent(**data)


def _result(value: str) -> ToolResult:
    data = json.loads(value)
    data["evidence_refs"] = tuple(data.get("evidence_refs", ()))
    data["artifact_refs"] = tuple(data.get("artifact_refs", ()))
    return ToolResult(**data)


def _message_status(message: Record) -> str:
    content = message.get("content")
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except (ValueError, TypeError):
            return "actual"
    if isinstance(content, dict):
        return str(cast(Record, content).get("status", "actual"))
    return "actual"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta(version INTEGER NOT NULL);
INSERT INTO schema_meta SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM schema_meta);
CREATE TABLE IF NOT EXISTS inputs(
 input_id TEXT PRIMARY KEY, event_key TEXT UNIQUE NOT NULL, event_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS contexts(
 context_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, controllers TEXT NOT NULL,
 address TEXT NOT NULL, access_json TEXT NOT NULL, control_state TEXT NOT NULL DEFAULT 'active',
 control_epoch INTEGER NOT NULL DEFAULT 1, input_seq INTEGER NOT NULL DEFAULT 0,
 history_version INTEGER NOT NULL DEFAULT 0, policy_version INTEGER NOT NULL,
 pending INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS context_inputs(
 context_id TEXT NOT NULL REFERENCES contexts, input_id TEXT NOT NULL REFERENCES inputs,
 seq INTEGER NOT NULL, PRIMARY KEY(context_id,input_id), UNIQUE(context_id,seq));
CREATE TABLE IF NOT EXISTS conversation_focus(
 address TEXT NOT NULL, author_id TEXT NOT NULL,
 context_id TEXT NOT NULL REFERENCES contexts,
 input_id TEXT NOT NULL REFERENCES inputs,
 PRIMARY KEY(address,author_id));
CREATE TABLE IF NOT EXISTS result_events(
 result_id TEXT PRIMARY KEY, context_id TEXT NOT NULL REFERENCES contexts,
 source_input_id TEXT NOT NULL REFERENCES inputs, seq INTEGER NOT NULL,
 payload TEXT NOT NULL, audience TEXT NOT NULL, UNIQUE(context_id,seq));
CREATE TABLE IF NOT EXISTS runs(
 run_id TEXT PRIMARY KEY, context_id TEXT NOT NULL REFERENCES contexts,
 status TEXT NOT NULL, control_epoch INTEGER NOT NULL, input_seq INTEGER NOT NULL,
 text TEXT NOT NULL DEFAULT '', outcome TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS one_running ON runs(context_id) WHERE status='running';
CREATE TABLE IF NOT EXISTS history(
 history_id INTEGER PRIMARY KEY AUTOINCREMENT, context_id TEXT NOT NULL REFERENCES contexts,
 run_id TEXT REFERENCES runs, call_id TEXT, result_status TEXT,
 audience TEXT NOT NULL, source_ids TEXT NOT NULL DEFAULT '[]', message TEXT NOT NULL,
 active INTEGER NOT NULL DEFAULT 1);
CREATE UNIQUE INDEX IF NOT EXISTS one_tool_result ON history(run_id,call_id)
 WHERE call_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS effects(
 effect_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs, call_id TEXT NOT NULL,
 tool TEXT NOT NULL, args TEXT NOT NULL, resource_keys TEXT NOT NULL,
 status TEXT NOT NULL, result TEXT, UNIQUE(run_id,call_id));
CREATE TABLE IF NOT EXISTS resource_claims(
 resource_key TEXT PRIMARY KEY, effect_id TEXT NOT NULL REFERENCES effects);
CREATE TABLE IF NOT EXISTS execution_processes(
 process_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs,
 context_id TEXT NOT NULL REFERENCES contexts, control_epoch INTEGER NOT NULL,
 status TEXT NOT NULL, details TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS deliveries(
 delivery_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs,
 context_id TEXT NOT NULL REFERENCES contexts, intent TEXT NOT NULL,
 status TEXT NOT NULL, input_seq INTEGER NOT NULL, control_epoch INTEGER NOT NULL,
 receipt TEXT);
CREATE TABLE IF NOT EXISTS delivery_batches(
 batch_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs, plan TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS delivery_batch_items(
 batch_id TEXT NOT NULL REFERENCES delivery_batches,
 ordinal INTEGER NOT NULL, delivery_id TEXT NOT NULL UNIQUE REFERENCES deliveries,
 PRIMARY KEY(batch_id,ordinal));
CREATE TABLE IF NOT EXISTS progress_notifications(
 context_id TEXT PRIMARY KEY REFERENCES contexts, attempted_at REAL NOT NULL,
 stage_key TEXT NOT NULL, delivery_id TEXT NOT NULL REFERENCES deliveries);
CREATE TABLE IF NOT EXISTS sent_messages(
 address TEXT NOT NULL, message_id TEXT NOT NULL, delivery_id TEXT NOT NULL REFERENCES deliveries,
 context_id TEXT NOT NULL REFERENCES contexts, inquiry_id TEXT, purpose TEXT NOT NULL,
 PRIMARY KEY(address,message_id));
CREATE TABLE IF NOT EXISTS inquiries(
 inquiry_id TEXT PRIMARY KEY, context_id TEXT NOT NULL REFERENCES contexts,
 run_id TEXT NOT NULL REFERENCES runs, recipient_id TEXT NOT NULL, address TEXT NOT NULL,
 question TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
 delivery_id TEXT NOT NULL REFERENCES deliveries, answer_input_id TEXT REFERENCES inputs);
CREATE TABLE IF NOT EXISTS inquiry_replacements(
 original_id TEXT PRIMARY KEY REFERENCES inquiries,
 replacement_id TEXT UNIQUE NOT NULL REFERENCES inquiries);
CREATE TABLE IF NOT EXISTS late_answers(
 inquiry_id TEXT NOT NULL REFERENCES inquiries, input_id TEXT NOT NULL REFERENCES inputs,
 context_id TEXT NOT NULL REFERENCES contexts, inquiry_status TEXT NOT NULL,
 PRIMARY KEY(inquiry_id,input_id));
CREATE TABLE IF NOT EXISTS artifacts(
 artifact_id TEXT NOT NULL, version INTEGER NOT NULL,
 context_id TEXT NOT NULL REFERENCES contexts, owner_id TEXT NOT NULL,
 audience TEXT NOT NULL, path TEXT NOT NULL, sha256 TEXT NOT NULL, source_ref TEXT,
 PRIMARY KEY(artifact_id,version));
CREATE TABLE IF NOT EXISTS router_notice_contexts(
 context_id TEXT PRIMARY KEY REFERENCES contexts);
CREATE TABLE IF NOT EXISTS router_notices(
 notice_id TEXT PRIMARY KEY, input_id TEXT NOT NULL REFERENCES inputs,
 context_id TEXT NOT NULL REFERENCES contexts, run_id TEXT UNIQUE NOT NULL REFERENCES runs,
 delivery_id TEXT UNIQUE NOT NULL REFERENCES deliveries);
"""


class StateStore:
    """Short transactions on a connection owned by the constructing thread."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._closed = False
        self._revoked_run_ids: set[str] = set()
        self._db = sqlite3.connect(self.path, timeout=0, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        try:
            self._db.execute("PRAGMA foreign_keys=ON")
            self._db.execute("PRAGMA busy_timeout=0")
            self._db.execute("PRAGMA journal_mode=WAL")
            exists = self._db.execute(
                "SELECT name FROM sqlite_master WHERE name='schema_meta'"
            ).fetchone()
            if exists:
                versions = self._db.execute("SELECT version FROM schema_meta").fetchall()
                if len(versions) != 1 or versions[0][0] != 1:
                    raise StateConflict("Unsupported state schema version")
            self._db.executescript("BEGIN IMMEDIATE;" + _SCHEMA + "COMMIT;")
        except BaseException as exc:
            self._db.close()
            self._closed = True
            if isinstance(exc, sqlite3.Error):
                raise StateConflict("State initialization failed") from exc
            raise

    def close(self) -> None:
        if not self._closed:
            self._db.close()
            self._closed = True

    @contextmanager
    def _transaction(self) -> Generator[None, None, None]:
        try:
            self._db.execute("BEGIN IMMEDIATE")
            yield
            self._db.commit()
        except BaseException as exc:
            if self._db.in_transaction:
                self._db.rollback()
            if isinstance(exc, sqlite3.Error):
                raise StateConflict("State transaction failed") from exc
            raise

    def _one(self, sql: str, params: tuple[Any, ...]) -> Record:
        row = self._db.execute(sql, params).fetchone()
        if row is None:
            raise StateConflict("State record not found")
        return dict(row)

    def ingest(self, event: InputEvent) -> tuple[str, bool]:
        input_id = _id("input", event.key)
        with self._transaction():
            row = self._db.execute(
                "SELECT input_id FROM inputs WHERE event_key=?", (event.key,)
            ).fetchone()
            if row:
                return str(row[0]), True
            self._db.execute(
                "INSERT INTO inputs VALUES(?,?,?)", (input_id, event.key, _json(asdict(event)))
            )
        return input_id, False

    def get_input(self, input_id: str) -> InputEvent:
        return _event(self._one("SELECT * FROM inputs WHERE input_id=?", (input_id,))["event_json"])

    def list_unbound_inputs(self) -> list[tuple[str, InputEvent]]:
        rows = self._db.execute(
            "SELECT i.input_id,i.event_json FROM inputs i WHERE NOT EXISTS "
            "(SELECT 1 FROM context_inputs c WHERE c.input_id=i.input_id) ORDER BY i.rowid"
        ).fetchall()
        return [(str(row["input_id"]), _event(row["event_json"])) for row in rows]

    def visible_message_sources(
        self, context_id: str, input_seq: int, message_ids: Iterable[str]
    ) -> tuple[Record, ...]:
        """Resolve actual messages in this context, never arbitrary platform IDs.

        Forwarded inner authors/IDs are claims inside content and are deliberately
        not added to this set. Acknowledged bot output is admissible only in the
        same context and destination as the current input visibility.
        """
        context = self.get_context(context_id)
        found: dict[str, Record] = {}
        for row in self.list_events(context_id):
            if row["kind"] != "input" or row["seq"] > input_seq:
                continue
            event: InputEvent = row["event"]
            if event.address != context["address"]:
                continue
            found[event.message_id] = {
                "message_id": event.message_id,
                "author_id": event.author_id,
                "input_id": row["input_id"],
                "source_kind": "received_message",
                "event": event,
            }
        for row in self._db.execute(
            "SELECT s.message_id,s.delivery_id FROM sent_messages s JOIN deliveries d "
            "ON s.delivery_id=d.delivery_id WHERE s.context_id=? AND s.address=? "
            "AND d.status='acknowledged' AND d.input_seq<=?",
            (context_id, context["address"].key, input_seq),
        ):
            found[str(row["message_id"])] = {
                "message_id": str(row["message_id"]),
                "author_id": context["address"].bot_id,
                "delivery_id": str(row["delivery_id"]),
                "source_kind": "acknowledged_delivery",
            }
        selected: list[Record] = []
        for message_id in message_ids:
            if message_id not in found:
                raise PermissionError("Message is outside this context's visible source set")
            selected.append(found[message_id])
        return tuple(selected)

    def create_context(
        self,
        context_id: str,
        event: InputEvent,
        access: AccessContext,
        controllers: Iterable[str] = (),
    ) -> None:
        with self._transaction():
            prior = self._db.execute(
                "SELECT * FROM contexts WHERE context_id=?", (context_id,)
            ).fetchone()
            if prior:
                if prior["owner_id"] != event.author_id or prior["address"] != event.address.key:
                    raise StateConflict("Context identity conflict")
                return
            self._db.execute(
                "INSERT INTO contexts(context_id,owner_id,controllers,address,access_json,"
                "policy_version) VALUES(?,?,?,?,?,?)",
                (
                    context_id,
                    event.author_id,
                    _json(sorted(set(controllers))),
                    event.address.key,
                    _json(_access_dict(access)),
                    access.policy_version,
                ),
            )

    def _attach(self, context_id: str, input_id: str) -> int:
        row = self._db.execute(
            "SELECT seq FROM context_inputs WHERE context_id=? AND input_id=?",
            (context_id, input_id),
        ).fetchone()
        if row:
            return int(row[0])
        context = self.get_context(context_id)
        self.get_input(input_id)
        seq = int(context["input_seq"]) + 1
        self._db.execute("INSERT INTO context_inputs VALUES(?,?,?)", (context_id, input_id, seq))
        self._db.execute(
            "UPDATE contexts SET input_seq=?,pending=1 WHERE context_id=?", (seq, context_id)
        )
        return seq

    def _set_conversation_focus(self, context_id: str, input_id: str) -> None:
        event = self.get_input(input_id)
        context = self.get_context(context_id)
        if context["kind"] != "work" or context["address"] != event.address:
            raise StateConflict("Conversation focus requires an actual same-address work input")
        if (
            self._db.execute(
                "SELECT 1 FROM context_inputs WHERE context_id=? AND input_id=?",
                (context_id, input_id),
            ).fetchone()
            is None
        ):
            raise StateConflict("Focus input is not attached to its context")
        # Delayed retries of an older human input must not steal a newer focus.
        self._db.execute(
            "INSERT INTO conversation_focus VALUES(?,?,?,?) "
            "ON CONFLICT(address,author_id) DO UPDATE SET context_id=excluded.context_id,input_id=excluded.input_id "
            "WHERE (SELECT rowid FROM inputs WHERE input_id=conversation_focus.input_id) "
            "<= (SELECT rowid FROM inputs WHERE input_id=excluded.input_id)",
            (event.address.key, event.author_id, context_id, input_id),
        )

    def conversation_focus(self, address: ConversationAddress, author_id: str) -> Record | None:
        """Load a persistent focus; older stores bootstrap from one latest scoped input.

        Policy and closed/paused behavior belong to the coordinator. Do not filter
        revoked contexts here: silently choosing an older one would bypass policy.
        """
        row = self._db.execute(
            "SELECT context_id FROM conversation_focus WHERE address=? AND author_id=?",
            (address.key, author_id),
        ).fetchone()
        if row:
            return self.get_context(str(row[0]))
        rows = self._db.execute(
            "SELECT ci.context_id,ci.input_id,i.event_json FROM context_inputs ci "
            "JOIN contexts c ON c.context_id=ci.context_id JOIN inputs i ON i.input_id=ci.input_id "
            "WHERE c.address=? AND NOT EXISTS(SELECT 1 FROM router_notice_contexts n "
            "WHERE n.context_id=c.context_id) ORDER BY i.rowid DESC,ci.rowid DESC",
            (address.key,),
        ).fetchall()
        candidates = [
            r
            for r in rows
            if (source := _event(r["event_json"])).address == address
            and source.author_id == author_id
        ]
        if not candidates:
            return None
        latest = candidates[0]
        if len({r["context_id"] for r in candidates if r["input_id"] == latest["input_id"]}) != 1:
            return None
        with self._transaction():
            self._set_conversation_focus(str(latest["context_id"]), str(latest["input_id"]))
        return self.get_context(str(latest["context_id"]))

    def attach_input(self, context_id: str, input_id: str, *, update_focus: bool = False) -> int:
        with self._transaction():
            seq = self._attach(context_id, input_id)
            if update_focus:
                self._set_conversation_focus(context_id, input_id)
            return seq

    def append_result_event(
        self,
        context_id: str,
        source_input_id: str,
        result_id: str,
        payload: Record,
        audience: str,
    ) -> int:
        """Persist a derived result without pretending that a human sent another message."""
        if not result_id or not audience or not isinstance(cast(object, payload), dict):
            raise ValueError("Result identity, scope and object payload are required")
        encoded = _json(payload)
        with self._transaction():
            context = self.get_context(context_id)
            if (
                self._db.execute(
                    "SELECT 1 FROM context_inputs WHERE context_id=? AND input_id=?",
                    (context_id, source_input_id),
                ).fetchone()
                is None
            ):
                raise StateConflict("Result source input does not belong to this context")
            old = self._db.execute(
                "SELECT * FROM result_events WHERE result_id=?", (result_id,)
            ).fetchone()
            if old:
                if (
                    old["context_id"] != context_id
                    or old["source_input_id"] != source_input_id
                    or old["payload"] != encoded
                    or old["audience"] != audience
                ):
                    raise StateConflict(
                        "Result identity was reused with different content or scope"
                    )
                return int(old["seq"])
            seq = int(context["input_seq"]) + 1
            self._db.execute(
                "INSERT INTO result_events VALUES(?,?,?,?,?,?)",
                (result_id, context_id, source_input_id, seq, encoded, audience),
            )
            self._db.execute(
                "UPDATE contexts SET input_seq=?,pending=1 WHERE context_id=?", (seq, context_id)
            )
            return seq

    def get_context(self, context_id: str) -> Record:
        row = self._one("SELECT * FROM contexts WHERE context_id=?", (context_id,))
        row["controllers"] = json.loads(row["controllers"])
        row["address"] = ConversationAddress(*json.loads(row["address"]))
        row["access"] = _access(row.pop("access_json"))
        row["kind"] = (
            "router_notice"
            if self._db.execute(
                "SELECT 1 FROM router_notice_contexts WHERE context_id=?", (context_id,)
            ).fetchone()
            else "work"
        )
        return row

    def list_contexts(self, address: ConversationAddress | None = None) -> list[Record]:
        rows = self._db.execute(
            "SELECT context_id FROM contexts" + (" WHERE address=?" if address else ""),
            (address.key,) if address else (),
        ).fetchall()
        return [self.get_context(str(row[0])) for row in rows]

    def resolve_input(self, address: ConversationAddress, message_id: str) -> Record | None:
        key = json.dumps([address.key, message_id], separators=(",", ":"))
        rows = self._db.execute(
            "SELECT i.input_id,c.context_id FROM inputs i JOIN context_inputs c "
            "ON c.input_id=i.input_id WHERE i.event_key=?",
            (key,),
        ).fetchall()
        return dict(rows[0]) if len(rows) == 1 else None

    def list_events(self, context_id: str, after_seq: int = 0) -> list[Record]:
        rows = self._db.execute(
            "SELECT c.seq,c.input_id,i.event_json,EXISTS(SELECT 1 FROM router_notices n "
            "WHERE n.context_id=c.context_id AND n.input_id=c.input_id) AS handled_by_notice "
            "FROM context_inputs c JOIN inputs i "
            "ON c.input_id=i.input_id WHERE c.context_id=? AND c.seq>? ORDER BY c.seq",
            (context_id, after_seq),
        ).fetchall()
        result: list[Record] = []
        for row in rows:
            event = _event(row["event_json"])
            result.append(
                {
                    "seq": row["seq"],
                    "event_id": row["input_id"],
                    "input_id": row["input_id"],
                    "event": event,
                    "kind": "input",
                    "handled_by_notice": bool(row["handled_by_notice"]),
                    "audience": event.address.key,
                    "source_ids": [row["input_id"]],
                }
            )
        for row in self._db.execute(
            "SELECT * FROM result_events WHERE context_id=? AND seq>? ORDER BY seq",
            (context_id, after_seq),
        ):
            result.append(
                {
                    "seq": row["seq"],
                    "event_id": "result:" + row["result_id"],
                    "kind": "runtime_result"
                    if json.loads(row["payload"]).get("kind") == "process_completion"
                    else "material_result",
                    "input_id": row["source_input_id"],
                    "event": None,
                    "payload": json.loads(row["payload"]),
                    "source_ids": [row["source_input_id"]],
                    "audience": row["audience"],
                }
            )
        return sorted(result, key=lambda item: int(item["seq"]))

    def set_controllers(self, context_id: str, owner_id: str, controllers: Iterable[str]) -> None:
        with self._transaction():
            if self.get_context(context_id)["owner_id"] != owner_id:
                raise StateConflict("Only the owner may change controllers")
            self._db.execute(
                "UPDATE contexts SET controllers=?,control_epoch=control_epoch+1 "
                "WHERE context_id=?",
                (_json(sorted(set(controllers))), context_id),
            )

    def claim_run(self, context_id: str, run_id: str) -> int:
        with self._transaction():
            context = self.get_context(context_id)
            if context["control_state"] != "active":
                raise RunHalted("Context is not active")
            epoch = int(context["control_epoch"])
            consumed = int(
                self._db.execute(
                    "SELECT COALESCE(MAX(r.input_seq),0) FROM runs r WHERE r.context_id=? "
                    "AND NOT EXISTS(SELECT 1 FROM router_notices n WHERE n.run_id=r.run_id)",
                    (context_id,),
                ).fetchone()[0]
            )
            self._db.execute(
                "INSERT INTO runs(run_id,context_id,status,control_epoch,input_seq) "
                "VALUES(?,?,'running',?,?)",
                (run_id, context_id, epoch, consumed),
            )
            self._db.execute("UPDATE contexts SET pending=0 WHERE context_id=?", (context_id,))
            return epoch

    def get_run(self, run_id: str) -> Record:
        row = self._one("SELECT * FROM runs WHERE run_id=?", (run_id,))
        row["outcome"] = json.loads(row["outcome"]) if row["outcome"] else None
        return row

    def control(self, context_id: str, actor_id: str, action: str) -> ControlReceipt:
        if action not in ("stop", "resume", "close"):
            raise ValueError("Unknown control action")
        with self._transaction():
            context = self.get_context(context_id)
            if actor_id not in {context["owner_id"], *context["controllers"]}:
                return ControlReceipt(False, int(context["control_epoch"]), "Not a controller")
            state = {"stop": "paused", "resume": "active", "close": "closed"}[action]
            epoch = int(context["control_epoch"]) + 1
            self._db.execute(
                "UPDATE contexts SET control_state=?,control_epoch=?,"
                "pending=CASE WHEN ?='active' THEN 1 ELSE pending END WHERE context_id=?",
                (state, epoch, state, context_id),
            )
            return ControlReceipt(True, epoch, "Control persisted; in-flight effects not reversed")

    def assert_current(self, run_id: str, epoch: int, input_seq: int | None = None) -> None:
        if run_id in self._revoked_run_ids:
            raise RunHalted("Run authority was revoked by this process")
        run = self.get_run(run_id)
        context = self.get_context(run["context_id"])
        if (
            run["status"] != "running"
            or context["control_state"] != "active"
            or run["control_epoch"] != epoch
            or context["control_epoch"] != epoch
        ):
            raise RunHalted("Run dispatch authority has expired")
        if input_seq is not None and input_seq != context["input_seq"]:
            raise StaleView("New input requires a refreshed model decision")

    def revoke_running_authority(self) -> list[str]:
        """Synchronously fence running handlers before asynchronous shutdown cleanup.

        The local fence survives a failed epoch transaction in this process. It
        neither terminates runs nor claims that in-flight effects were cancelled.
        The caller invokes this once per shutdown and records terminal outcomes.
        """
        running = self._db.execute(
            "SELECT run_id,context_id FROM runs WHERE status='running'"
        ).fetchall()
        self._revoked_run_ids.update(str(row["run_id"]) for row in running)
        contexts = sorted({str(row["context_id"]) for row in running})
        with self._transaction():
            for context_id in contexts:
                self._db.execute(
                    "UPDATE contexts SET control_epoch=control_epoch+1 WHERE context_id=?",
                    (context_id,),
                )
        return contexts

    def consume_inputs(self, run_id: str, input_seq: int) -> None:
        with self._transaction():
            run = self.get_run(run_id)
            self.assert_current(run_id, run["control_epoch"], input_seq)
            self._db.execute("UPDATE runs SET input_seq=? WHERE run_id=?", (input_seq, run_id))
            self._db.execute(
                "UPDATE contexts SET pending=0 WHERE context_id=?", (run["context_id"],)
            )

    def append_history(self, run_id: str, message: Record) -> None:
        if message.get("role") == "tool":
            call_id = message.get("tool_call_id")
            if not isinstance(call_id, str) or not call_id:
                raise ValueError("Tool result requires tool_call_id")
            self.upsert_tool_result(run_id, call_id, message)
            return
        with self._transaction():
            run = self.get_run(run_id)
            context = self.get_context(run["context_id"])
            if message.get("_audience", context["access"].audience) != context["access"].audience:
                raise StateConflict("History audience mismatch")
            sources = list(message.get("_covered_input_ids", []))
            if message.get("_input_id"):
                sources.append(message["_input_id"])
            if not all(isinstance(source, str) for source in sources):
                raise ValueError("Invalid history source identifiers")
            self._db.execute(
                "INSERT INTO history(context_id,run_id,audience,source_ids,message) "
                "VALUES(?,?,?,?,?)",
                (
                    run["context_id"],
                    run_id,
                    context["access"].audience,
                    _json(list(dict.fromkeys(sources))),
                    _json(message),
                ),
            )
            self._bump_history(run["context_id"])

    def _bump_history(self, context_id: str) -> None:
        self._db.execute(
            "UPDATE contexts SET history_version=history_version+1 WHERE context_id=?",
            (context_id,),
        )

    def history(self, context_id: str) -> list[Record]:
        return [
            json.loads(row[0])
            for row in self._db.execute(
                "SELECT message FROM history WHERE context_id=? AND active=1 ORDER BY history_id",
                (context_id,),
            )
        ]

    def history_records(self, context_id: str) -> list[Record]:
        return [
            {
                "message": json.loads(r["message"]),
                "audience": r["audience"],
                "source_ids": json.loads(r["source_ids"]),
                "run_id": r["run_id"],
                "active": bool(r["active"]),
            }
            for r in self._db.execute(
                "SELECT * FROM history WHERE context_id=? ORDER BY history_id", (context_id,)
            )
        ]

    def upsert_tool_result(self, run_id: str, call_id: str, result_message: Record) -> None:
        if result_message.get("role") != "tool" or result_message.get("tool_call_id") != call_id:
            raise ValueError("Tool-result identity mismatch")
        with self._transaction():
            self._upsert_tool_result(run_id, call_id, result_message)

    def _upsert_tool_result(self, run_id: str, call_id: str, result_message: Record) -> None:
        run = self.get_run(run_id)
        context = self.get_context(run["context_id"])
        old = self._db.execute(
            "SELECT * FROM history WHERE run_id=? AND call_id=?", (run_id, call_id)
        ).fetchone()
        status = _message_status(result_message)
        if old:
            if old["result_status"] != "unknown" and status == "unknown":
                return
            if old["message"] == _json(result_message):
                return
            if old["result_status"] != "unknown":
                raise StateConflict("Conflicting terminal tool result")
            self._db.execute(
                "UPDATE history SET message=?,result_status=? WHERE history_id=?",
                (_json(result_message), status, old["history_id"]),
            )
        else:
            self._db.execute(
                "INSERT INTO history(context_id,run_id,call_id,result_status,audience,message)"
                " VALUES(?,?,?,?,?,?)",
                (
                    run["context_id"],
                    run_id,
                    call_id,
                    status,
                    context["access"].audience,
                    _json(result_message),
                ),
            )
        self._bump_history(run["context_id"])

    def _recover_tool_results(self, run_id: str) -> list[str]:
        rows = self._db.execute(
            "SELECT message FROM history WHERE run_id=? AND active=1 ORDER BY history_id",
            (run_id,),
        ).fetchall()
        closed: list[str] = []
        for row in rows:
            message = json.loads(row[0])
            if message.get("role") != "assistant":
                continue
            for call in message.get("tool_calls", []):
                call_id = call.get("id")
                if not isinstance(call_id, str) or not call_id:
                    raise StateConflict("Persisted assistant call has no identity")
                existing = self._db.execute(
                    "SELECT result_status FROM history WHERE run_id=? AND call_id=?",
                    (run_id, call_id),
                ).fetchone()
                if existing and existing[0] != "unknown":
                    continue
                effect = self._db.execute(
                    "SELECT * FROM effects WHERE run_id=? AND call_id=?", (run_id, call_id)
                ).fetchone()
                if effect is None:
                    result = ToolResult("not_executed", "recovered_before_dispatch")
                elif effect["status"] in ("attempting", "unknown"):
                    result = ToolResult(
                        "unknown",
                        "recovered_effect_requires_reconciliation",
                        effect_ref=effect["effect_id"],
                    )
                elif effect["result"]:
                    result = _result(effect["result"])
                else:
                    raise StateConflict("Terminal effect has no persisted result")
                self._upsert_tool_result(
                    run_id,
                    call_id,
                    {
                        "role": "tool",
                        "tool_call_id": call_id,
                        "content": result.to_json(),
                    },
                )
                closed.append(run_id + ":" + call_id)
        return closed

    def create_process(self, process_id: str, run_id: str, details: Record) -> None:
        """Durable spawn intent, written before creating any operating-system process."""
        with self._transaction():
            run = self.get_run(run_id)
            self.assert_current(run_id, run["control_epoch"])
            self._db.execute(
                "INSERT INTO execution_processes VALUES(?,?,?,?,?,?)",
                (
                    process_id,
                    run_id,
                    run["context_id"],
                    run["control_epoch"],
                    "starting",
                    _json(details),
                ),
            )

    def update_process(self, process_id: str, status: str, details: Record) -> None:
        if status not in {"starting", "running", "exited", "terminated", "failed", "unknown"}:
            raise ValueError("Invalid execution process state")
        with self._transaction():
            if (
                self._db.execute(
                    "UPDATE execution_processes SET status=?, details=? WHERE process_id=?",
                    (status, _json(details), process_id),
                ).rowcount
                != 1
            ):
                raise StateConflict("Unknown execution process")

    def list_processes(self, context_id: str | None = None) -> list[Record]:
        rows = self._db.execute(
            "SELECT * FROM execution_processes" + (" WHERE context_id=?" if context_id else ""),
            (context_id,) if context_id else (),
        ).fetchall()
        return [dict(row, details=json.loads(row["details"])) for row in rows]

    def recover_processes(self) -> int:
        """A PID persisted by an earlier owner is not proof of a live process identity."""
        with self._transaction():
            return self._db.execute(
                "UPDATE execution_processes SET status='unknown' WHERE status IN ('starting','running')"
            ).rowcount

    def begin_effect(
        self,
        run_id: str,
        call_id: str,
        tool: str,
        args: Record,
        resource_keys: Iterable[str],
        input_seq: int,
    ) -> str:
        effect_id = _id("effect", run_id + ":" + call_id)
        keys = sorted(set(resource_keys))
        with self._transaction():
            run = self.get_run(run_id)
            self.assert_current(run_id, run["control_epoch"], input_seq)
            if not keys:
                fingerprint = hashlib.sha256(_json([tool, args]).encode()).hexdigest()
                keys = [f"implicit:{run['context_id']}:{fingerprint}"]
            for key in keys:
                if (
                    key.startswith("qq:")
                    and self._db.execute(
                        "SELECT 1 FROM progress_notifications p JOIN deliveries d "
                        "ON p.delivery_id=d.delivery_id JOIN contexts c ON c.context_id=p.context_id "
                        "WHERE c.address=? AND d.status IN ('prepared','attempting','unknown')",
                        (key.removeprefix("qq:"),),
                    ).fetchone()
                ):
                    raise StateConflict("QQ progress delivery has an in-flight or unknown result")
                if self._db.execute(
                    "SELECT 1 FROM resource_claims WHERE resource_key=?", (key,)
                ).fetchone():
                    raise StateConflict("Resource has an in-flight or unknown effect")
            self._db.execute(
                "INSERT INTO effects VALUES(?,?,?,?,?,?,'attempting',NULL)",
                (effect_id, run_id, call_id, tool, _json(args), _json(keys)),
            )
            self._db.executemany(
                "INSERT INTO resource_claims VALUES(?,?)", [(key, effect_id) for key in keys]
            )
            self._db.execute("UPDATE runs SET input_seq=? WHERE run_id=?", (input_seq, run_id))
        return effect_id

    def get_effect(self, effect_id: str) -> Record:
        row = self._one("SELECT * FROM effects WHERE effect_id=?", (effect_id,))
        row["args"] = json.loads(row["args"])
        row["resource_keys"] = json.loads(row["resource_keys"])
        row["result"] = _result(row["result"]) if row["result"] else None
        return row

    def list_effects(self, context_id: str | None = None) -> list[Record]:
        rows = self._db.execute(
            "SELECT e.effect_id FROM effects e JOIN runs r ON e.run_id=r.run_id"
            + (" WHERE r.context_id=?" if context_id else ""),
            (context_id,) if context_id else (),
        ).fetchall()
        return [self.get_effect(str(row[0])) for row in rows]

    def finish_effect(self, effect_id: str, result: ToolResult) -> None:
        with self._transaction():
            self._finish_effect(effect_id, result)

    def _finish_effect(self, effect_id: str, result: ToolResult) -> ToolResult:
        if result.status not in ("succeeded", "failed", "not_executed", "needs_input", "unknown"):
            raise ValueError("Invalid effect status")
        old = self.get_effect(effect_id)
        if old["status"] not in ("attempting", "unknown"):
            if result.status == "unknown" or old["result"] == result:
                return cast(ToolResult, old["result"])
            raise StateConflict("Conflicting terminal effect result")
        self._db.execute(
            "UPDATE effects SET status=?,result=? WHERE effect_id=?",
            (result.status, result.to_json(), effect_id),
        )
        if result.status != "unknown":
            self._db.execute("DELETE FROM resource_claims WHERE effect_id=?", (effect_id,))
        return result

    def finish_effect_with_history(self, effect_id: str, result: ToolResult) -> None:
        """Commit actual effect truth, resource release and its model result together."""
        with self._transaction():
            effect = self.get_effect(effect_id)
            effective = self._finish_effect(effect_id, result)
            self._upsert_tool_result(
                effect["run_id"],
                effect["call_id"],
                {
                    "role": "tool",
                    "tool_call_id": effect["call_id"],
                    "content": effective.to_json(),
                },
            )

    def _prepare_delivery(
        self,
        run_id: str,
        intent: OutboundIntent,
        input_seq: int,
        *,
        identity_override: str | None = None,
    ) -> str:
        if len(intent.artifact_refs) > 1 or (intent.artifact_refs and intent.text):
            raise ValueError("One delivery intent must represent one send item")
        run = self.get_run(run_id)
        if intent.forward_message_ids:
            context = self.get_context(run["context_id"])
            if intent.address != context["address"]:
                raise PermissionError("Merged forwarding must remain in the source conversation")
            self.visible_message_sources(run["context_id"], input_seq, intent.forward_message_ids)
        value = _json(asdict(intent))
        identity = identity_override or (
            run_id + (":final" if intent.purpose == "final" else ":" + value)
        )
        delivery_id = _id("delivery", identity)
        old = self._db.execute(
            "SELECT intent FROM deliveries WHERE delivery_id=?", (delivery_id,)
        ).fetchone()
        if old:
            if old[0] != value:
                raise StateConflict("Stable delivery identity has different content")
            return delivery_id
        self._db.execute(
            "INSERT INTO deliveries VALUES(?,?,?,?, 'prepared',?,?,NULL)",
            (delivery_id, run_id, run["context_id"], value, input_seq, run["control_epoch"]),
        )
        return delivery_id

    def prepare_delivery(self, run_id: str, intent: OutboundIntent, input_seq: int) -> str:
        with self._transaction():
            return self._prepare_delivery(run_id, intent, input_seq)

    def prepare_progress(
        self,
        run_id: str,
        epoch: int,
        input_seq: int,
        stage_key: str,
        text: str,
        now: float,
        min_interval: float,
    ) -> str | None:
        """Persist a rate-limited phase notice without bypassing unresolved QQ effects."""
        if (
            not stage_key
            or not text.strip()
            or not math.isfinite(now)
            or not math.isfinite(min_interval)
            or min_interval < 0
        ):
            raise ValueError("Invalid progress notice")
        with self._transaction():
            self.assert_current(run_id, epoch, input_seq)
            run = self.get_run(run_id)
            context = self.get_context(run["context_id"])
            if self._db.execute(
                "SELECT 1 FROM resource_claims WHERE resource_key=?",
                ("qq:" + context["address"].key,),
            ).fetchone():
                return None
            if self._db.execute(
                "SELECT 1 FROM progress_notifications p JOIN deliveries d "
                "ON p.delivery_id=d.delivery_id JOIN contexts c ON c.context_id=p.context_id "
                "WHERE c.address=? AND d.status IN ('prepared','attempting','unknown')",
                (context["address"].key,),
            ).fetchone():
                return None
            previous = self._db.execute(
                "SELECT * FROM progress_notifications WHERE context_id=?", (run["context_id"],)
            ).fetchone()
            if previous:
                old = self.get_delivery(str(previous["delivery_id"]))
                if old["status"] in {"prepared", "attempting", "unknown"}:
                    return None
                if now - float(previous["attempted_at"]) < min_interval:
                    return None
                if previous["stage_key"] == stage_key:
                    return None
            intent = OutboundIntent(context["address"], text, "progress")
            delivery_id = self._prepare_delivery(
                run_id, intent, input_seq, identity_override=f"progress:{run_id}:{stage_key}"
            )
            self._db.execute(
                "INSERT INTO progress_notifications VALUES(?,?,?,?) ON CONFLICT(context_id) DO UPDATE SET attempted_at=excluded.attempted_at,stage_key=excluded.stage_key,delivery_id=excluded.delivery_id",
                (run["context_id"], now, stage_key, delivery_id),
            )
            return delivery_id

    def _prepare_delivery_items(
        self,
        run_id: str,
        intents: tuple[OutboundIntent, ...],
        input_seq: int,
        *,
        final: bool = False,
    ) -> tuple[str, ...]:
        if not intents:
            return ()
        plan = _json([asdict(intent) for intent in intents])
        batch_id = _id("delivery_batch", run_id + (":final" if final else ":" + plan))
        old = self._db.execute(
            "SELECT plan FROM delivery_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if old:
            if old[0] != plan:
                raise StateConflict("Stable delivery batch has different items")
            return tuple(
                str(row[0])
                for row in self._db.execute(
                    "SELECT delivery_id FROM delivery_batch_items WHERE batch_id=? ORDER BY ordinal",
                    (batch_id,),
                )
            )
        self._db.execute("INSERT INTO delivery_batches VALUES(?,?,?)", (batch_id, run_id, plan))
        ids: list[str] = []
        for index, intent in enumerate(intents):
            if final:
                identity = run_id + (":final" if index == 0 else f":final:{index}")
            else:
                identity = f"{batch_id}:{index}"
            delivery_id = self._prepare_delivery(
                run_id, intent, input_seq, identity_override=identity
            )
            self._db.execute(
                "INSERT INTO delivery_batch_items VALUES(?,?,?)", (batch_id, index, delivery_id)
            )
            ids.append(delivery_id)
        return tuple(ids)

    def prepare_delivery_items(
        self, run_id: str, intents: Iterable[OutboundIntent], input_seq: int
    ) -> tuple[str, ...]:
        """Persist every item and its ordering in one transaction; formatting belongs to caller."""
        items = tuple(intents)
        with self._transaction():
            return self._prepare_delivery_items(run_id, items, input_seq)

    def list_delivery_items(self, delivery_id: str) -> list[Record]:
        """Return the complete ordered batch, or a one-item legacy delivery."""
        item = self._db.execute(
            "SELECT batch_id FROM delivery_batch_items WHERE delivery_id=?", (delivery_id,)
        ).fetchone()
        if item is None:
            return [self.get_delivery(delivery_id)]
        return [
            self.get_delivery(str(row[0]))
            for row in self._db.execute(
                "SELECT delivery_id FROM delivery_batch_items WHERE batch_id=? ORDER BY ordinal",
                (item[0],),
            )
        ]

    def prepare_notice(
        self,
        input_id: str,
        text: str,
        access: AccessContext,
        context_id: str | None = None,
        *,
        update_focus: bool = False,
        start_conversation: bool = False,
    ) -> str:
        """Persist one reply from the deterministic router without a model/tool run."""
        if not text.strip():
            raise ValueError("Notice text must not be empty")
        with self._transaction():
            event = self.get_input(input_id)
            if access.actor_id != event.author_id or access.audience != event.address.key:
                raise StateConflict(
                    "Notice access does not match the real input identity or address"
                )
            independent = context_id is None
            destination_context = context_id or _id("notice_context", input_id)
            notice_id = _id("notice", _json([input_id, text, event.address.key]))
            previous = self._db.execute(
                "SELECT * FROM router_notices WHERE notice_id=?",
                (notice_id,),
            ).fetchone()
            if previous:
                if previous["context_id"] != destination_context:
                    raise StateConflict("Notice identity already belongs to a different context")
                return str(previous["delivery_id"])

            created = False
            if independent:
                exists = self._db.execute(
                    "SELECT 1 FROM contexts WHERE context_id=?",
                    (destination_context,),
                ).fetchone()
                if not exists:
                    self._db.execute(
                        "INSERT INTO contexts(context_id,owner_id,controllers,address,access_json,"
                        "policy_version) VALUES(?,?,'[]',?,?,?)",
                        (
                            destination_context,
                            event.author_id,
                            event.address.key,
                            _json(_access_dict(access)),
                            access.policy_version,
                        ),
                    )
                    self._db.execute(
                        "INSERT INTO router_notice_contexts VALUES(?)",
                        (destination_context,),
                    )
                    created = True
                context = self.get_context(destination_context)
                if (
                    context["kind"] != "router_notice"
                    or context["owner_id"] != event.author_id
                    or context["address"] != event.address
                ):
                    raise StateConflict("Independent notice context identity conflict")
            else:
                context = self.get_context(destination_context)
                if context["address"] != event.address or event.author_id not in {
                    context["owner_id"],
                    *context["controllers"],
                }:
                    raise StateConflict(
                        "Linked notice requires an authorized same-address controller"
                    )

            seq = self._attach(destination_context, input_id)
            if start_conversation and (independent or seq != 1 or context["input_seq"] != 0):
                raise StateConflict("New conversation confirmation requires an empty work context")
            if update_focus:
                self._set_conversation_focus(destination_context, input_id)
            run_id = _id("notice_run", notice_id)
            outcome = _json(
                {
                    "kind": "router_notice",
                    "status": "yielded",
                    "text": text,
                    "input_id": input_id,
                    "consumed_input_ids": [input_id],
                }
            )
            self._db.execute(
                "INSERT INTO runs(run_id,context_id,status,control_epoch,input_seq,text,outcome) "
                "VALUES(?,?,'yielded',?,?,?,?)",
                (run_id, destination_context, context["control_epoch"], seq, text, outcome),
            )
            intent = OutboundIntent(event.address, text, "control", reply_to=event.message_id)
            delivery_id = self._prepare_delivery(run_id, intent, seq)
            self._db.execute(
                "INSERT INTO router_notices VALUES(?,?,?,?,?)",
                (notice_id, input_id, destination_context, run_id, delivery_id),
            )
            if created or start_conversation:
                # This context only contains the notice input; do not schedule a model for it.
                self._db.execute(
                    "UPDATE contexts SET pending=0 WHERE context_id=?",
                    (destination_context,),
                )
            return delivery_id

    def get_delivery(self, delivery_id: str) -> Record:
        row = self._one("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
        row["intent"] = _intent(row["intent"])
        row["receipt"] = ProtocolReceipt(**json.loads(row["receipt"])) if row["receipt"] else None
        batch = self._db.execute(
            "SELECT batch_id,ordinal FROM delivery_batch_items WHERE delivery_id=?", (delivery_id,)
        ).fetchone()
        if batch is not None:
            row["batch_id"], row["ordinal"] = str(batch[0]), int(batch[1])
        return row

    def list_deliveries(
        self,
        status: str | None = None,
        *,
        context_id: str | None = None,
        order_by_confirmation: bool = False,
    ) -> list[Record]:
        predicates: list[str] = []
        values: list[str] = []
        if status is not None:
            predicates.append("status=?")
            values.append(status)
        if context_id is not None:
            predicates.append("context_id=?")
            values.append(context_id)
        if order_by_confirmation and status != "acknowledged":
            raise ValueError("Confirmation ordering requires acknowledged deliveries")
        query = (
            "SELECT d.delivery_id,(SELECT MIN(s.rowid) FROM sent_messages s "
            "WHERE s.delivery_id=d.delivery_id AND s.context_id=d.context_id) AS confirmation_order "
            "FROM deliveries d"
            if order_by_confirmation
            else "SELECT delivery_id FROM deliveries"
        )
        query += " WHERE " + " AND ".join(predicates) if predicates else ""
        if order_by_confirmation:
            # sent_messages is inserted in the same transaction as the first actual
            # acknowledged receipt, independently of when delivery intents were prepared.
            # Ack records without message IDs have no known order; put them in an
            # explicitly labelled stable tail rather than inventing a confirmation time.
            query += " ORDER BY confirmation_order IS NULL,confirmation_order,d.rowid"
        rows = self._db.execute(query, values).fetchall()
        result: list[Record] = []
        for row in rows:
            item = self.get_delivery(str(row[0]))
            if order_by_confirmation:
                item["confirmation_order"] = row["confirmation_order"]
            result.append(item)
        return result

    def claim_delivery(self, delivery_id: str) -> Record | None:
        with self._transaction():
            delivery = self.get_delivery(delivery_id)
            if delivery["status"] != "prepared":
                return None
            context = self.get_context(delivery["context_id"])
            intent: OutboundIntent = delivery["intent"]
            access: AccessContext = context["access"]
            is_notice = (
                self._db.execute(
                    "SELECT 1 FROM router_notices WHERE delivery_id=?",
                    (delivery_id,),
                ).fetchone()
                is not None
            )
            valid = (
                context["control_epoch"] == delivery["control_epoch"]
                and (is_notice or context["input_seq"] == delivery["input_seq"])
                and (context["control_state"] == "active" or intent.purpose == "control")
                and (
                    intent.purpose != "progress"
                    or self.get_run(delivery["run_id"])["status"] == "running"
                )
                and (
                    intent.address == context["address"]
                    or intent.address.key in access.allowed_recipients
                )
            )
            if not valid:
                self._db.execute(
                    "UPDATE deliveries SET status='superseded' WHERE delivery_id=?", (delivery_id,)
                )
                return None
            if "batch_id" in delivery:
                pending_prefix = self._db.execute(
                    "SELECT 1 FROM delivery_batch_items i JOIN deliveries d ON d.delivery_id=i.delivery_id "
                    "WHERE i.batch_id=? AND i.ordinal<? AND d.status!='acknowledged' LIMIT 1",
                    (delivery["batch_id"], delivery["ordinal"]),
                ).fetchone()
                if pending_prefix is not None:
                    return None
            self._db.execute(
                "UPDATE deliveries SET status='attempting' WHERE delivery_id=?", (delivery_id,)
            )
            return self.get_delivery(delivery_id)

    def finish_delivery(self, delivery_id: str, receipt: ProtocolReceipt) -> None:
        if receipt.status not in ("acknowledged", "failed", "unknown"):
            raise ValueError("Invalid receipt status")
        with self._transaction():
            old = self.get_delivery(delivery_id)
            if old["status"] not in ("attempting", "unknown"):
                if old["receipt"] == receipt or (
                    old["status"] == "acknowledged" and receipt.status == "unknown"
                ):
                    return
                raise StateConflict("Delivery has no active attempt")
            self._db.execute(
                "UPDATE deliveries SET status=?,receipt=? WHERE delivery_id=?",
                (receipt.status, _json(asdict(receipt)), delivery_id),
            )
            if receipt.status == "acknowledged" and receipt.message_id:
                intent: OutboundIntent = old["intent"]
                prior = self._db.execute(
                    "SELECT delivery_id FROM sent_messages WHERE address=? AND message_id=?",
                    (intent.address.key, receipt.message_id),
                ).fetchone()
                if prior and prior[0] != delivery_id:
                    raise StateConflict("Message receipt maps to a different delivery")
                self._db.execute(
                    "INSERT OR IGNORE INTO sent_messages VALUES(?,?,?,?,?,?)",
                    (
                        intent.address.key,
                        receipt.message_id,
                        delivery_id,
                        old["context_id"],
                        intent.inquiry_id,
                        intent.purpose,
                    ),
                )

    def _finish_run_record(
        self,
        run_id: str,
        status: str,
        text: str,
        *,
        reason: str = "",
        unresolved_effects: tuple[str, ...] = (),
    ) -> Record:
        if status not in ("completed", "yielded", "failed", "cancelled", "interrupted"):
            raise ValueError("Invalid run status")
        run = self.get_run(run_id)
        if self._db.execute("SELECT 1 FROM router_notices WHERE run_id=?", (run_id,)).fetchone():
            raise StateConflict("Router notice outcomes are already terminal")
        unresolved = sorted(
            set(unresolved_effects)
            | {
                str(row[0])
                for row in self._db.execute(
                    "SELECT effect_id FROM effects WHERE run_id=? AND status IN ('attempting','unknown')",
                    (run_id,),
                )
            }
        )
        outcome_value = {
            "status": status,
            "text": text,
            "reason": reason,
            "unresolved_effects": unresolved,
        }
        if run["status"] != "running":
            old_outcome: Record = run["outcome"] or {}
            if (
                run["status"] != status
                or run["text"] != text
                or old_outcome.get("reason", "") != reason
                or sorted(old_outcome.get("unresolved_effects", [])) != unresolved
            ):
                raise StateConflict("Conflicting terminal run outcome")
        outcome = _json(outcome_value)
        self._db.execute(
            "UPDATE runs SET status=?,text=?,outcome=? WHERE run_id=?",
            (status, text, outcome, run_id),
        )
        return run

    def finish_run_items(
        self,
        run_id: str,
        status: str,
        text: str = "",
        final_intents: Iterable[OutboundIntent] = (),
        *,
        reason: str = "",
        unresolved_effects: tuple[str, ...] = (),
    ) -> tuple[str, ...]:
        """Commit a terminal result and all ordered delivery items together or not at all."""
        items = tuple(final_intents)
        with self._transaction():
            run = self._finish_run_record(
                run_id, status, text, reason=reason, unresolved_effects=unresolved_effects
            )
            return self._prepare_delivery_items(run_id, items, int(run["input_seq"]), final=True)

    def finish_run(
        self,
        run_id: str,
        status: str,
        text: str = "",
        final_intent: OutboundIntent | None = None,
        *,
        reason: str = "",
        unresolved_effects: tuple[str, ...] = (),
    ) -> str | None:
        with self._transaction():
            run = self._finish_run_record(
                run_id, status, text, reason=reason, unresolved_effects=unresolved_effects
            )
            return (
                self._prepare_delivery(run_id, final_intent, int(run["input_seq"]))
                if final_intent
                else None
            )

    def resolve_sent(self, address: ConversationAddress, message_id: str) -> Record | None:
        row = self._db.execute(
            "SELECT context_id,inquiry_id,delivery_id,purpose FROM sent_messages "
            "WHERE address=? AND message_id=?",
            (address.key, message_id),
        ).fetchone()
        return dict(row) if row else None

    def create_inquiry(
        self,
        run_id: str,
        recipient_id: str,
        address: ConversationAddress,
        question: str,
        input_seq: int,
        inquiry_id: str | None = None,
    ) -> tuple[str, str]:
        inquiry_id = inquiry_id or "inquiry_" + uuid.uuid4().hex
        with self._transaction():
            return self._create_inquiry(
                run_id, recipient_id, address, question, input_seq, inquiry_id
            )

    def _create_inquiry(
        self,
        run_id: str,
        recipient_id: str,
        address: ConversationAddress,
        question: str,
        input_seq: int,
        inquiry_id: str,
    ) -> tuple[str, str]:
        run = self.get_run(run_id)
        self.assert_current(run_id, run["control_epoch"], input_seq)
        old = self._db.execute(
            "SELECT * FROM inquiries WHERE inquiry_id=?",
            (inquiry_id,),
        ).fetchone()
        if old:
            if (
                old["run_id"] != run_id
                or old["recipient_id"] != recipient_id
                or old["address"] != address.key
                or old["question"] != question
            ):
                raise StateConflict("Inquiry identity conflict")
            return inquiry_id, str(old["delivery_id"])
        context = self.get_context(run["context_id"])
        access: AccessContext = context["access"]
        if address != context["address"] and address.key not in access.allowed_recipients:
            raise StateConflict("Inquiry recipient is outside allowed scope")
        if address.kind == "private" and address.target_id != recipient_id:
            raise StateConflict("Private inquiry identity mismatch")
        intent = OutboundIntent(address, question, "inquiry", inquiry_id=inquiry_id)
        delivery_id = self._prepare_delivery(run_id, intent, input_seq)
        self._db.execute(
            "INSERT INTO inquiries VALUES(?,?,?,?,?,?,'open',?,NULL)",
            (
                inquiry_id,
                run["context_id"],
                run_id,
                recipient_id,
                address.key,
                question,
                delivery_id,
            ),
        )
        return inquiry_id, delivery_id

    def _invalidate_inquiry(self, inquiry_id: str, status: str) -> None:
        inquiry = self.get_inquiry(inquiry_id)
        if inquiry["status"] != "open":
            raise StateConflict("Only an open inquiry can be changed")
        self._db.execute("UPDATE inquiries SET status=? WHERE inquiry_id=?", (status, inquiry_id))
        self._db.execute(
            "UPDATE deliveries SET status='superseded' WHERE delivery_id=? AND status='prepared'",
            (inquiry["delivery_id"],),
        )
        self._bump_history(inquiry["context_id"])

    def cancel_inquiry(self, run_id: str, inquiry_id: str, input_seq: int) -> Record:
        with self._transaction():
            run = self.get_run(run_id)
            self.assert_current(run_id, run["control_epoch"], input_seq)
            inquiry = self.get_inquiry(inquiry_id)
            if inquiry["context_id"] != run["context_id"]:
                raise StateConflict("Inquiry belongs to another context")
            if inquiry["status"] == "cancelled":
                return inquiry
            self._invalidate_inquiry(inquiry_id, "cancelled")
            return self.get_inquiry(inquiry_id)

    def replace_inquiry(
        self,
        run_id: str,
        inquiry_id: str,
        recipient_id: str,
        address: ConversationAddress,
        question: str,
        input_seq: int,
        replacement_id: str | None = None,
    ) -> tuple[str, str]:
        replacement_id = replacement_id or "inquiry_" + uuid.uuid4().hex
        if replacement_id == inquiry_id:
            raise ValueError("A replacement must have a new inquiry identity")
        with self._transaction():
            run = self.get_run(run_id)
            self.assert_current(run_id, run["control_epoch"], input_seq)
            original = self.get_inquiry(inquiry_id)
            if original["context_id"] != run["context_id"]:
                raise StateConflict("Inquiry belongs to another context")
            prior = self._db.execute(
                "SELECT replacement_id FROM inquiry_replacements WHERE original_id=?",
                (inquiry_id,),
            ).fetchone()
            if prior:
                replacement = self.get_inquiry(str(prior[0]))
                if (
                    prior[0] != replacement_id
                    or replacement["recipient_id"] != recipient_id
                    or replacement["address"] != address
                    or replacement["question"] != question
                ):
                    raise StateConflict("Inquiry has already been replaced differently")
                return replacement_id, str(replacement["delivery_id"])
            self._invalidate_inquiry(inquiry_id, "superseded")
            created, delivery_id = self._create_inquiry(
                run_id,
                recipient_id,
                address,
                question,
                input_seq,
                replacement_id,
            )
            self._db.execute("INSERT INTO inquiry_replacements VALUES(?,?)", (inquiry_id, created))
            return created, delivery_id

    def record_late_answer(self, inquiry_id: str, input_id: str) -> Record:
        with self._transaction():
            inquiry = self.get_inquiry(inquiry_id)
            event = self.get_input(input_id)
            if (
                inquiry["status"] not in ("superseded", "cancelled")
                or event.author_id != inquiry["recipient_id"]
                or event.address != inquiry["address"]
            ):
                raise StateConflict("Late answer identity, address or inquiry state mismatch")
            delivery = self.get_delivery(inquiry["delivery_id"])
            receipt: ProtocolReceipt | None = delivery["receipt"]
            if (
                event.reply_to
                and receipt
                and receipt.message_id
                and event.reply_to != receipt.message_id
            ):
                raise StateConflict("Late answer quotes a different message")
            self._db.execute(
                "INSERT OR IGNORE INTO late_answers VALUES(?,?,?,?)",
                (inquiry_id, input_id, inquiry["context_id"], inquiry["status"]),
            )
            return next(
                row for row in self.list_late_answers(inquiry_id) if row["input_id"] == input_id
            )

    def list_late_answers(
        self,
        inquiry_id: str | None = None,
        *,
        context_id: str | None = None,
    ) -> list[Record]:
        clauses: list[str] = []
        params: list[str] = []
        if inquiry_id is not None:
            clauses.append("l.inquiry_id=?")
            params.append(inquiry_id)
        if context_id is not None:
            clauses.append("l.context_id=?")
            params.append(context_id)
        sql = (
            "SELECT l.*,i.event_json FROM late_answers l JOIN inputs i ON l.input_id=i.input_id"
            + (" WHERE " + " AND ".join(clauses) if clauses else "")
            + " ORDER BY l.rowid"
        )
        result: list[Record] = []
        for row in self._db.execute(sql, tuple(params)):
            event = _event(row["event_json"])
            result.append(
                {
                    "inquiry_id": row["inquiry_id"],
                    "input_id": row["input_id"],
                    "context_id": row["context_id"],
                    "inquiry_status": row["inquiry_status"],
                    "event": event,
                    "source_ids": [row["input_id"]],
                    "audience": event.address.key,
                }
            )
        return result

    def get_inquiry(self, inquiry_id: str) -> Record:
        row = self._one("SELECT * FROM inquiries WHERE inquiry_id=?", (inquiry_id,))
        row["address"] = ConversationAddress(*json.loads(row["address"]))
        return row

    def answer_inquiry(
        self, inquiry_id: str, input_id: str, *, update_focus: bool = False
    ) -> tuple[str, int]:
        with self._transaction():
            inquiry = self.get_inquiry(inquiry_id)
            event = self.get_input(input_id)
            if event.author_id != inquiry["recipient_id"] or event.address != inquiry["address"]:
                raise StateConflict("Inquiry answer identity or address mismatch")
            if inquiry["status"] == "answered" and inquiry["answer_input_id"] == input_id:
                seq = self._attach(inquiry["context_id"], input_id)
                if update_focus:
                    self._set_conversation_focus(inquiry["context_id"], input_id)
                return inquiry["context_id"], seq
            if inquiry["status"] != "open":
                raise StateConflict("Inquiry is no longer open")
            delivery = self.get_delivery(inquiry["delivery_id"])
            if delivery["status"] != "acknowledged":
                raise StateConflict("Inquiry delivery is not acknowledged yet")
            receipt: ProtocolReceipt = delivery["receipt"]
            if event.reply_to and receipt.message_id and event.reply_to != receipt.message_id:
                raise StateConflict("Reply points to a different message")
            seq = self._attach(inquiry["context_id"], input_id)
            if update_focus:
                self._set_conversation_focus(inquiry["context_id"], input_id)
            self._db.execute(
                "UPDATE inquiries SET status='answered',answer_input_id=? WHERE inquiry_id=?",
                (input_id, inquiry_id),
            )
            return inquiry["context_id"], seq

    def list_open_inquiries(self, context_id: str | None = None) -> list[Record]:
        sql = "SELECT inquiry_id FROM inquiries WHERE status='open'"
        rows = self._db.execute(
            sql + (" AND context_id=?" if context_id else ""), (context_id,) if context_id else ()
        ).fetchall()
        return [self.get_inquiry(str(row[0])) for row in rows]

    def set_inquiry_status(self, inquiry_id: str, status: str) -> None:
        if status not in ("superseded", "cancelled"):
            raise ValueError("Invalid inquiry replacement status")
        with self._transaction():
            inquiry = self.get_inquiry(inquiry_id)
            if inquiry["status"] == status:
                return
            self._invalidate_inquiry(inquiry_id, status)

    def recover(self) -> dict[str, list[str]]:
        with self._transaction():
            running = self._db.execute(
                "SELECT run_id,context_id FROM runs WHERE status='running'"
            ).fetchall()
            unknown_effects = [
                str(r[0])
                for r in self._db.execute("SELECT effect_id FROM effects WHERE status='attempting'")
            ]
            unknown_deliveries = [
                str(r[0])
                for r in self._db.execute(
                    "SELECT delivery_id FROM deliveries WHERE status='attempting'"
                )
            ]
            for context_id in {str(r["context_id"]) for r in running}:
                self._db.execute(
                    "UPDATE contexts SET control_epoch=control_epoch+1,"
                    "pending=CASE WHEN control_state='active' THEN 1 ELSE pending END "
                    "WHERE context_id=?",
                    (context_id,),
                )
            self._db.execute("UPDATE runs SET status='interrupted' WHERE status='running'")
            self._db.execute("UPDATE effects SET status='unknown' WHERE status='attempting'")
            self._db.execute("UPDATE deliveries SET status='unknown' WHERE status='attempting'")
            closed_calls = [
                call for row in running for call in self._recover_tool_results(str(row["run_id"]))
            ]
            consumed_sql = (
                "COALESCE((SELECT MAX(r.input_seq) FROM runs r "
                "WHERE r.context_id=c.context_id AND NOT EXISTS "
                "(SELECT 1 FROM router_notices n WHERE n.run_id=r.run_id)),0)"
            )
            active = [
                str(r[0])
                for r in self._db.execute(
                    "SELECT context_id FROM contexts c WHERE control_state='active' AND pending=1 "
                    "AND (EXISTS(SELECT 1 FROM context_inputs ci WHERE ci.context_id=c.context_id "
                    f"AND ci.seq>{consumed_sql} AND NOT EXISTS "
                    "(SELECT 1 FROM router_notices n WHERE n.context_id=c.context_id "
                    "AND n.input_id=ci.input_id)) OR EXISTS "
                    "(SELECT 1 FROM result_events re WHERE re.context_id=c.context_id "
                    f"AND re.seq>{consumed_sql}))"
                )
            ]
            interrupted = [
                str(row[0])
                for row in self._db.execute(
                    "SELECT c.context_id FROM contexts c WHERE c.control_state='active' "
                    "AND NOT EXISTS(SELECT 1 FROM router_notice_contexts nc "
                    "WHERE nc.context_id=c.context_id) AND (SELECT r.status FROM runs r "
                    "WHERE r.context_id=c.context_id AND NOT EXISTS "
                    "(SELECT 1 FROM router_notices n WHERE n.run_id=r.run_id) "
                    "ORDER BY r.rowid DESC LIMIT 1)='interrupted' ORDER BY c.rowid"
                )
            ]
            return {
                "interrupted_runs": [str(r["run_id"]) for r in running],
                "unknown_effects": unknown_effects,
                "unknown_deliveries": unknown_deliveries,
                "active_contexts": active,
                "interrupted_contexts": interrupted,
                "closed_tool_calls": closed_calls,
            }

    def compare_and_swap_history(
        self,
        context_id: str,
        expected_version: int,
        messages: list[Record],
        *,
        expected_epoch: int | None = None,
        expected_input_seq: int | None = None,
        expected_policy_version: int | None = None,
    ) -> bool:
        with self._transaction():
            context = self.get_context(context_id)
            if (
                context["history_version"] != expected_version
                or (expected_epoch is not None and context["control_epoch"] != expected_epoch)
                or (expected_input_seq is not None and context["input_seq"] != expected_input_seq)
                or (
                    expected_policy_version is not None
                    and context["policy_version"] != expected_policy_version
                )
            ):
                return False
            if self._db.execute(
                "SELECT 1 FROM effects e JOIN runs r ON e.run_id=r.run_id "
                "WHERE r.context_id=? AND e.status IN ('attempting','unknown')",
                (context_id,),
            ).fetchone():
                return False
            self._db.execute("UPDATE history SET active=0 WHERE context_id=?", (context_id,))
            self._db.executemany(
                "INSERT INTO history(context_id,audience,source_ids,message) VALUES(?,?,?,?)",
                [
                    (
                        context_id,
                        context["access"].audience,
                        _json(message.get("_covered_input_ids", [])),
                        _json(message),
                    )
                    for message in messages
                ],
            )
            self._bump_history(context_id)
            return True

    def register_artifact(
        self,
        artifact_id: str,
        context_id: str,
        owner_id: str,
        audience: str,
        path: str,
        sha256: str,
        version: int = 1,
        source_ref: str | None = None,
    ) -> None:
        if version < 1 or not path or len(sha256) != 64:
            raise ValueError("Invalid artifact metadata")
        try:
            int(sha256, 16)
        except ValueError as exc:
            raise ValueError("Invalid artifact checksum") from exc
        with self._transaction():
            old = self._db.execute(
                "SELECT * FROM artifacts WHERE artifact_id=? ORDER BY version DESC LIMIT 1",
                (artifact_id,),
            ).fetchone()
            values = (
                artifact_id,
                version,
                context_id,
                owner_id,
                audience,
                path,
                sha256,
                source_ref,
            )
            if old:
                if tuple(old) == values:
                    return
                if (
                    version <= old["version"]
                    or context_id != old["context_id"]
                    or owner_id != old["owner_id"]
                    or audience != old["audience"]
                ):
                    raise StateConflict("Artifact version or scope conflict")
            self._db.execute("INSERT INTO artifacts VALUES(?,?,?,?,?,?,?,?)", values)
            self._bump_history(context_id)

    def get_artifact(self, artifact_id: str, version: int | None = None) -> Record:
        sql = "SELECT * FROM artifacts WHERE artifact_id=?"
        return self._one(
            sql + (" AND version=?" if version is not None else " ORDER BY version DESC LIMIT 1"),
            (artifact_id, version) if version is not None else (artifact_id,),
        )
