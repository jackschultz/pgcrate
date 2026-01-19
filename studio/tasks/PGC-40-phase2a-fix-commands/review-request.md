# Phase 2a Code Review Request

## Context

**Project:** pgcrate-dba-diagnostics
**Branch:** `feature/phase2a-fix-commands`
**Base:** `c7d46cb` (Phase 1: JSON contracts foundation)
**Commits to review:** 3 commits totaling ~3,600 lines added

This PR implements the fix command system for pgcrate—the ability to not just diagnose PostgreSQL issues but remediate them safely.

---

## pgcrate Vision (from VISION.md)

pgcrate is a **PostgreSQL CLI for application developers** with these principles:

1. **Developer-first** - For app developers who manage their own DBs, not DBAs
2. **Safe defaults** - Read-only by default, explicit flags for mutations
3. **Diagnose-first** - Diagnostics identify issues, fixes require explicit action
4. **Machine-readable** - JSON output for automation and LLM integration
5. **Evidence-based** - Actions include evidence (stats, measurements) supporting recommendations

The fix command system embodies the **diagnose → fix → verify** workflow:
- Diagnostics surface issues with evidence
- Fix commands show exact SQL that will run (`--dry-run`)
- Gates prevent accidental execution (`--read-write`, `--primary`, `--yes`)
- Verification confirms the fix worked

---

## Commits

### 1. `4d3bfaf` - Phase 2a: Add vacuum diagnostic and fix commands

Core implementation:
- `src/commands/fix/` module with common types, sequence/index/vacuum fixes
- `src/commands/vacuum.rs` - New vacuum diagnostic
- Gate system for safe execution
- Verification runner for post-fix validation

### 2. `3b1dfad` - Phase 2a: Add integration tests for fix commands

- `tests/diagnostics/fix.rs` - 12 integration tests covering:
  - Sequence upgrade (smallint→integer→bigint)
  - Index drop with safety checks
  - Vacuum operations
  - Gate enforcement
  - Dry-run behavior

### 3. `a807cec` - Phase 2a: Update documentation for fix commands

- README.md - Added vacuum and fix command sections
- CHANGELOG.md - Phase 2a release notes
- llms.txt - Updated for LLM consumption

---

## Files to Review

### Core Implementation (priority)

| File | Lines | Purpose |
|------|-------|---------|
| `src/commands/fix/common.rs` | 335 | StructuredAction, ActionGates, Risk levels, gate checking |
| `src/commands/fix/sequence.rs` | 391 | ALTER SEQUENCE type upgrades |
| `src/commands/fix/index.rs` | 431 | DROP INDEX CONCURRENTLY with safety checks |
| `src/commands/fix/vacuum.rs` | 443 | VACUUM operations (regular/freeze/full/analyze) |
| `src/commands/fix/verify.rs` | 443 | JSONPath-based verification runner |
| `src/commands/vacuum.rs` | 482 | Vacuum health diagnostic |

### Integration Points

| File | Purpose |
|------|---------|
| `src/main.rs` | CLI routing for fix subcommands |
| `src/commands/triage.rs` | `--include-fixes` flag, action generation |
| `src/commands/indexes.rs` | Enhanced evidence collection |

### Tests

| File | Purpose |
|------|---------|
| `tests/diagnostics/fix.rs` | Integration tests for all fix commands |

---

## Review Focus

Please evaluate:

### 1. Architecture
- Does the gate system (`requires_write`, `requires_primary`, `requires_confirmation`) provide appropriate safety?
- Is the StructuredAction pattern clear and well-designed?
- Does the separation between diagnostics (read-only) and fixes (mutations) make sense?

### 2. Code Quality
- Any remaining bloat or unnecessary abstractions?
- Are error messages helpful?
- Is the SQL generation safe (identifier quoting, etc.)?

### 3. Safety
- Are the safety checks in `fix/index.rs` sufficient? (primary key, replica identity, backing constraint)
- Is the risk level assignment appropriate? (Low for ALTER SEQUENCE, Medium for DROP INDEX, High for VACUUM FULL)
- Any SQL injection vectors?

### 4. Testing
- Do the integration tests cover the important paths?
- Any edge cases missing?

### 5. Documentation
- Is the README clear on how to use fix commands?
- Are the CLI help messages helpful?

---

## Commands to Explore

```bash
cd /Users/jackschultz/workspace/dev/pgcrate-studio/pgcrate-dba-diagnostics

# View commits
git log --oneline c7d46cb..a807cec
git show 4d3bfaf --stat
git show 3b1dfad --stat
git show a807cec --stat

# Read core files
cat src/commands/fix/common.rs
cat src/commands/fix/sequence.rs
cat src/commands/fix/index.rs

# Run tests
cargo test --test integration fix

# See CLI help
cargo run -- fix --help
cargo run -- fix sequence --help
cargo run -- vacuum --help
```

---

## Known Decisions

These were intentional choices:

1. **JSONPath evaluation is simple** - Uses basic path extraction, not full JSONPath spec. Complex conditions like `&&` or `<` are not supported.

2. **Only Fix action type** - Removed Investigate/Monitor variants as they weren't used. Can add back if needed.

3. **Three risk levels** - Removed None/Extreme as they weren't used. Low/Medium/High covers current operations.

4. **VACUUM FULL requires --yes** - Despite being a standard operation, ACCESS EXCLUSIVE lock warrants explicit confirmation.
