# Optional QQ peer probe

The original driver is a deterministic test member, not another Agent. It supports Q01 roundtrip and optional Q03 quoting of P's actually received S reply. The separate [candidate lifecycle driver](CANDIDATE-PROBE.md) implements Q04 barriers, Q05 private-file content hashes and Q06 actual Inquiry/run inspection using an in-process candidate and scripted model. Neither implements Q07's third real identity.

Examples use synthetic account IDs and deliberately contain null endpoints. Replace the IDs in a private `*.local.json` profile with independently verified test accounts. A running process or WebUI address is not a usable authenticated OneBot endpoint. No existing connection may be replaced to make a probe work; a fresh checkout has no live-test evidence.

Ordinary pytest excludes `qq_live`. The loopback tests in `test_probe_offline.py` use synthetic accounts and a local fake OneBot peer; they do not produce real-platform evidence.

## Explicit future batch

Prepare a separate local profile from `peer-profile.example.json`, fill only P's independently configured **forward universal WebSocket** endpoint, and put its token in the environment variable named by `token_env`. No token value belongs in JSON or reports. Confirm identities, target and actual S engine/model before setting the opt-in. A group batch requires explicit `destination=group` and `group_id`; the driver queries both members first and mentions S. There is no group discovery or automatic selection.

From the repository root, after a real batch is authorized and its environment exists:

```text
.venv/Scripts/python.exe tools/qq_live_probe.py --allow-live --profile <local-profile.json> --output <local-evidence.json>
```

Without both options, or without an endpoint/token reference, the CLI returns `not_run` without connecting. It never configures NapCat. The actual pytest entry additionally needs `QQ_ALLOW_LIVE=1` and `QQ_LIVE_PROFILE=<local-profile.json>` and explicit selection:

```text
.venv/Scripts/python.exe -m pytest tests/live_qq/test_real_peer.py -m qq_live -q
```

Missing prerequisites or inconclusive observations are reported as skips, not passes. A test exception or rejected identity is a failure. Reports contain fixed reason codes, nonce, account-scoped message references and counts; no tokens, connection URL query, unrelated message bodies or raw exception dumps.

## Evidence and limits

The observer starts before get_login_info and before any stimulus. Account mismatch prevents sending. Requests register their response future before writing the socket. Each case has a distinct nonce; only real incoming message events from the expected S account and exact conversation count as replies. Q03 uses P's received message ID, never P's prior send receipt ID or a guessed S-side ID.

`pass` means **peer_observed_platform_roundtrip** only. `full_chain_verified` remains false; S's internal intake/model/tool/delivery trace is not available through P's endpoint. Labels `engine` and `model` are profile declarations, not independently verified facts. Combine this report with genuine S-side evidence to claim full chain or candidate-engine verification. A fake callback injected into S never qualifies.

Two accounts mean one bot and one test member. Q07 still requires a third QQ identity. The driver does not simulate a second human by changing nickname or sender_id.

No action is automatically retried. An unknown send may still have a matching observed reply; the report preserves both facts and stops remaining stimuli. Limits cap this driver's sends plus S messages actually observed in the target. They cannot force S to stop unseen/autonomous sends; unexpected output exhausts the observer budget and stops the batch. Case/batch timeouts are bounded and finite; local state-machine stress belongs in offline tests, not live message floods.
