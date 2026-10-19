# Standalone CLI and report-preservation regression

This note preserves the delivery-check failure and its correction. It is developer CLI evidence, not a real QQ run.

Before the fix, from `fusion/` with `PYTHONPATH` removed, `.verify-venv/Scripts/python.exe tools/qq_candidate_probe.py` exited 1 with:

```text
File ".../fusion/tools/qq_candidate_probe.py", line 30, in <module>
    from tools.qq_live_probe import PeerClient, ProbeProfile, ProbeStopped, message_ref
ModuleNotFoundError: No module named 'tools'
```

The earlier `python -m pytest` run made the project directory importable and tested package imports. A direct script invocation instead places the script's `tools/` directory at the front of the Python search path. An editable installation supplies the `src/` package, not the top-level development `tools` namespace. Consequently, the 16 passing application-driver tests did not establish that the file entrypoint worked. The candidate now selects a sibling import for direct script execution and a relative package import for module execution. It does not add arbitrary paths or change runtime code.

Both CLI entrypoints now reserve an output file with exclusive `x` creation before profile loading or any connection. An existing file, directory, or concurrently created path causes `not_run` without overwriting it. Failed report writing still emits the complete result to stdout with a sanitized output-failure marker. A newly reserved file may be empty/partial after a process or storage failure; it is retained rather than reused, so the next attempt needs a fresh report path.

`test_probe_cli.py` clears `PYTHONPATH` for subprocess checks of both drivers in file and module mode. It checks existing evidence preservation, new `not_run` reports, an exclusive-create race before runner invocation, and stdout retention after a file-write error. No tests connect to actual QQ.

The first combined regression run had 2 failed and 42 passed: the new test expected an incorrect legacy opt-in reason string. Both entrypoints already returned valid `not_run` JSON. The test now asserts the original `explicit_allow_live_and_profile_required` value for the peer driver and the candidate's distinct `explicit_profile_and_opt_in_required` value. The correction changes the test oracle, not the drivers' established reason codes.

Final scoped validation on Windows Python 3.12 with `.verify-venv/Scripts/python.exe`: Ruff check passed; Ruff format check reported all 3 files formatted; `pytest tests/live_qq/test_probe_cli.py tests/live_qq/test_probe_offline.py tests/live_qq/test_candidate_probe_offline.py -q` completed with **44 passed in 4.33s**. The 11 new CLI checks supplement the 33 existing offline peer/candidate checks. Whole-tree and Linux validation remain the root workstream's subsequent checks; earlier source fingerprints precede this fix.
