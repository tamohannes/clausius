# Testing Logbook

## 14 Mar 2026

Split tests into CI-safe (mock cluster) and local-dev (real cluster) modes.

- `server/config.py` now falls back to `config.example.json` when `config.json` is absent
- All unit/integration tests use injected `mock-cluster` instead of reading real cluster names
- Removed all `from server.config import CLUSTERS` + `next(c for c in CLUSTERS ...)` patterns from non-live tests
- Added `mock_cluster` fixture (always available) and `first_real_cluster` fixture (skips on CI)
- Added `local_cluster` pytest marker for tests that need real cluster config
- Verified **240 tests pass with config.json absent** (CI simulation)
- Live tests auto-detect first cluster from config.json, overridable via `TEST_CLUSTER`

## 13 Mar 2026

Initial comprehensive test suite implemented after full codebase refactor.

- **240 deterministic tests passing** (133 unit + 69 integration + 38 MCP) in ~5s
- **19 live tests** ready (read + destructive + MCP boundary)
- Frontend unit tests (Vitest) and E2E tests (Playwright) written; pending Node.js install
- All tests isolated: temp DBs, cache resets, mocked SSH/subprocess
- MCP-to-Flask boundary tests use real Flask server in background thread
- Live destructive tests use throwaway Slurm jobs with cleanup finalizers
- `cancel_all` excluded from live suite per policy

Coverage by module:
- `server/db.py`: parsers, time normalization, upsert/dismiss visibility, dependency inference, repin
- `server/jobs.py`: dependency parsing, squeue output parsing, sort order
- `server/logs.py`: progress extraction, label/sort, arg parsing, JSONL index/record
- `server/mounts.py`: path resolution, mount status, dir listing, mount script
- `server/config.py`: cache TTL, mount map loading, settings response, example-config fallback
- `server/routes.py`: all 18 API endpoints (success + error paths)
- `mcp_server.py`: all 7 tools + 1 resource, transport errors, URL encoding, boundary params

Open items:
- Install Node.js to run Vitest and Playwright suites
- Add GitHub Actions workflow for CI
