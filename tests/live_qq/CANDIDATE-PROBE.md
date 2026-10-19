# Q04/Q05/Q06 candidate lifecycle driver

This is a development test driver, not a second product Agent or a production bridge. It starts one actual `Application` in this Python process with a deterministic `ScriptModel`, the normal runtime/state/tool/delivery modules, and a fresh state directory. P still sends through its independently authenticated OneBot endpoint; S receives through its real authenticated reverse WebSocket. Direct calls to `Coordinator.accept` cannot qualify as live input.

## What is implemented

- **Q04:** S sends an actual `ask_person` question containing the case nonce. The driver waits for P's actual observation and S's second model-entry barrier. P quotes its own observed message ID to send a correction; the old scripted call must become `not_executed`. After S actually enters the next model call and consumes the correction, P quotes the question with a stop command. S must become paused with a cancelled run. Timing is established by model-entry events and persisted state, not an arbitrary sleep.
- **Q05:** private-file roundtrip only. P sends a small synthetic UTF-8 fixture from an explicitly shared directory. S must actually obtain/read it, create a derived artifact through `create_material`, and send the pinned version. P independently obtains the output URL and downloads the bytes. The report compares source/S-read hashes, S-output/P-download hashes and persisted source lineage. Names and successful API receipts alone do not pass. Group upload behavior is not implemented by this driver and returns `not_run` before sending.
- **Q06:** S uses the real `ask_person` and `wait_for_input` tools. The driver observes the question at P and a yielded S run, then sends P's real quoted answer. The persisted Inquiry must identify P and the continuation must use a different run, with P observing its final reply.
- **Q07:** not implemented; two different human participants require a third real QQ identity. Synthetic nicknames cannot fill that requirement.

## Fail-closed prerequisites

Copy `candidate-profile.example.json` to a private local profile and supply actual account IDs. Its null defaults do not connect or send. The nested peer fields reuse the Q01/Q03 reader's identity, nonce, account-scoped message IDs and bounded outbound accounting; its engine/model labels are not used as S evidence.

`candidate_config` is an explicitly prepared candidate Settings JSON. It must select S's exact account, only P as allowed user, and only the selected group (or no groups for private tests). Enable `ask_person` and `wait_for_input` for Q04/Q06; Q05 additionally needs `read_material`, `create_material`, `send_material`, `napcat_shared_local_files=true`, an exact trusted download-host allowlist and `peer_shared_dir`.

`isolation_config` is a read-only path to the operator-supplied S NapCat OneBot network configuration. The driver checks one enabled reverse WebSocket entry targeting the candidate's configured `/onebot` endpoint, and rejects other enabled HTTP, SSE or WebSocket entries and unsupported network shapes. It never edits that file or configures NapCat. This evidence covers the supplied file plus live S/P login checks; it is not proof that no process or consumer exists outside that configuration. The operator must provide the already authorized isolated environment. Do not change the existing Pi connection just to make this preflight pass.

Live mode requires a fixed candidate port. A missing/unsupported config, token reference, endpoint, tool permission or required file-sharing configuration returns `not_run` before any platform stimulus. No connected/verified S account means `not_run`, not a fabricated source probe. The model has no real provider API calls in this driver.

The configured candidate `data_dir` is never opened or modified: the test application uses a new random **sibling** directory under the existing parent. Collisions and linked configured roots are refused. Synthetic P fixtures get a new nonce directory within the explicitly shared directory. Result/state artifacts are retained for review; the driver never deletes real chats, original data, or shared services.

## Invocation

From `fusion/`, without live opt-in (no connection or listener):

```text
.venv/Scripts/python.exe -m tools.qq_candidate_probe
```

After the isolated account configuration, references and batch are ready:

```text
.venv/Scripts/python.exe -m tools.qq_candidate_probe --allow-live --profile var/candidate-profile.local.json --output var/candidate-probe-result.json
```

Use the module invocation above so the sibling development tools are importable. Credential values come only from the environment variables referenced by Settings and the peer profile; reports do not include tokens, endpoint query strings or arbitrary message bodies.

No SSH connection, remote configuration change, production service replacement, second autonomous model, or HTTP bridge is performed. If the driver runs on a different host from NapCat without a genuinely shared filesystem, Q05 cannot be made valid by setting a boolean; leave it disabled until the paths really exist on the relevant hosts.

## Evidence and limits

The driver checks that imported `Application` source belongs to the inspected source tree, records that tree's fingerprint, and rejects a source change during the batch. It checks S's live `get_login_info`, then P's, before sending. Group cases also check both memberships through P. S evidence comes from the in-process application, actual model-entry callbacks and persisted input/Inquiry/run/effect/delivery/artifact records. It does not come from a profile saying “fusion”.

Reports retain completed cases, partial failed-case references, uncertain send receipts and model-stage observations. A combined budget guards P sends and candidate S sends; the peer observer separately bounds received S traffic. Unknown sends are never retried. Case and batch deadlines are total bounds, not merely per-read timeouts. Failure to enter the required stage is not a pass.

`tests/live_qq/test_candidate_probe_offline.py` connects the actual application through a loopback-only platform fixture. The fixture translates account-scoped quote IDs and observes messages before sender acknowledgements; it does not call runtime intake directly. It also tests wrong/missing identities, missing file prerequisites, extra consumers, budget exhaustion, unknown sends, tampered file bytes and preservation of an existing data directory. Such results always say `transport=simulated`, `qq_live_verified=false`, and `full_chain_verified=false`.

Only an actual live run with all case assertions and shutdown checks passing can set `qq_live_verified/full_chain_verified=true`. This still proves only the selected scripted-model cases, not live-model judgment, general concurrency, power-loss durability or arbitrary third-party tool cancellation. No real Q04/Q05/Q06 run was performed while implementing this driver.
