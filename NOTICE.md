# Source and license notices

ShittimChest combines selected source adaptations within one project. The project
metadata identifies AGPL-3.0-or-later; it must not be described as wholly MIT.

## AstrBot

- Repository: https://github.com/AstrBotDevs/AstrBot
- Reviewed source commit: `e5e21b71aec5642f110eb115eb8e9a0d8028b7e5`
- Upstream metadata declares AGPL-3.0-or-later. The complete preserved AGPLv3 text is
  copied verbatim to [LICENSE](LICENSE) and also retained in
  [third_party/LICENSES/AstrBot-AGPL-3.0-or-later.txt](third_party/LICENSES/AstrBot-AGPL-3.0-or-later.txt).
- Selected QQ conversion, dispatch and filename/URI behavior was adapted into
  project-owned types and lifecycle. Paths, hashes and changes are recorded in
  [astrbot-port-manifest.json](third_party/astrbot-port-manifest.json) and source notes.
  The filename helper's exact source/target hashes are also recorded in
  [material-context-port-manifest.json](third_party/material-context-port-manifest.json).

AstrBot's FIRST_NOTICE and EULA statements are preserved under `third_party/LICENSES/`
as upstream evidence. They are not instructions for this Agent, its QQ persona, or
new application copy. These upstream AstrBot statements are preserved as provenance,
not imposed as additional ShittimChest license conditions. ShittimChest does not
adopt AstrBot's privacy, security, commercial-service, or product commitments.
The source license remains AGPL-3.0-or-later; preservation of the statements alone
does not resolve every question about their scope.

## Hermes Agent

- Repository: https://github.com/NousResearch/hermes-agent
- Reviewed source commit: `9e01b44dd23978f9ff40b7837467f3a9f1582130`
- Copyright (c) 2025 Nous Research. Selected source is provided under MIT; its full
  notice is retained in [Hermes-MIT.txt](third_party/LICENSES/Hermes-MIT.txt).
- Ported algorithms include stream tool-call accumulation, budgets, repetition
  checks, response conversion, loop ordering and tool-group compaction boundaries.
  The full upstream application is not embedded. See
  [hermes-port-manifest.json](third_party/hermes-port-manifest.json) and source headers.
  The separately extracted compaction boundary and candidate-commit algorithms are
  recorded in [material-context-port-manifest.json](third_party/material-context-port-manifest.json).

### Current capability integrations and attribution boundaries

- [Execution](third_party/hermes-execution-manifest.json): selected upstream native
  file, patch, fuzzy matching, escape correction and output-truncation algorithms.
  Workspace policy, process supervision, Windows Job Objects, bubblewrap policy,
  the effect journal and delivery lifecycle are application-owned integrations.
- [Replay](third_party/hermes-replay-manifest.json): reasoning carriers, stream
  aggregation, call-ID normalization and route-specific replay adaptations.
- [Runtime skills](third_party/hermes-skills-manifest.json): selected metadata
  algorithms plus an application-owned, scoped skill-library adapter.
- [MCP](third_party/hermes-mcp-manifest.json): official MCP SDK sessions and
  application-owned registration/effect adapters; selected Hermes metadata filters.
- [Web](third_party/hermes-web-manifest.json),
  [vision](third_party/hermes-vision-manifest.json): each manifest distinguishes
  copied or adapted algorithms from source-informed integrations using independent
  dependencies.
- [QQ forwards](third_party/astrbot-forward-manifest.json): AstrBot-informed
  forwarding and attribution behavior integrated with this application's outbox.

`AgentEngine` is an application-owned execution loop with selected Hermes-derived
algorithms and ordering invariants. It is not a wholesale port of Hermes `AIAgent`,
`run_agent.py`, its full provider matrix, CLI, gateway, or all upstream capabilities.
The presence of a source manifest does not establish feature parity. Historical W
validation records remain observations of their original candidate; current source
hashes and R evidence describe later adaptations separately.

Installed wheels include these notices, lock files and source manifests beneath
`share/shittim-core` in the installation prefix, alongside the wheel's standard
`dist-info/licenses` records. External Chromium and bubblewrap executables are not
bundled in the Python wheel. Those binaries and Python dependencies retain their
own licenses and separate installation requirements.

## Research and development references

PeopleJoin and Co-Gym informed the collaboration design. The candidate does not ship
their experiment runtimes or datasets as a consequence of that design reference.
Development skills and private research records are not included in this repository
and do not become bot prompts.

Retain the source/license records when copying adapted code. Dependencies and any
future selected third-party files keep their own notices; a root license does not
establish the license of every optional upstream plugin or dataset. This notice does
not claim that a public release, deployment or production cutover has occurred.
