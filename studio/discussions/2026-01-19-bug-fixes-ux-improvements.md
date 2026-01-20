# Bug Fixes and UX Improvements from Agent Feedback

**Date:** 2026-01-19
**Source:** builder-studio agent feedback analysis (106 feedback issues)
**Status:** Bugs resolved, UX improvements pending

---

## Priority 1: Bugs (All Resolved)

### BUG-1: UTF-8 String Slicing in sequences.rs ✅ FIXED

**Fixed in:** commit `9e8dcbd` (2026-01-18)

Now uses `chars().count()` and `chars().take()` for safe UTF-8 handling.

---

### BUG-2: xid_age Type Overflow in triage.rs ✅ FIXED

**Fixed in:** commit `9e8dcbd` (2026-01-18)

Now uses `i64` consistently for xid_age across all commands.

---

### BUG-3: Empty Database Handling in xid Command ✅ FIXED

**Fixed in:** commit `9e8dcbd` (2026-01-18)

Added proper `.context()` error handling and empty result handling.

---

### BUG-4: Triage Sequences Display Inconsistency ✅ FIXED

**Fixed in:** v0.4.0 (2026-01-19)

Changed triage query to use `round(..., 2)::float8` matching sequences.rs.

---

## Priority 2: UX Improvements

### UX-1: --verbose Should Show SQL Queries

**Feedback:** "Initially tried to use --verbose flag to see the underlying SQL queries but it didn't output the SQL"

**Location:** Diagnostic commands don't log SQL even with --verbose

**Fix:** Add SQL logging when verbose=true. In diagnostic functions, print queries before execution:
```rust
if verbose {
    eprintln!("-- Executing SQL:");
    eprintln!("{}", query);
}
```

**Files to update:**
- `src/commands/triage.rs`
- `src/commands/xid.rs`
- `src/commands/sequences.rs`
- `src/commands/locks.rs`
- `src/commands/indexes.rs`
- `src/commands/bloat.rs`
- `src/commands/replication.rs`
- `src/commands/vacuum.rs`

---

### UX-2: Command Naming Aliases

**Feedback:** 25+ reports of trying `pgcrate migration create` instead of `pgcrate migrate new`

**Options:**
1. Add `migration` as alias for `migrate` subcommand
2. Add helpful error message suggesting correct command
3. Add `create` as alias for `new`

**Recommendation:** Add aliases for common mistakes:
- `migration` → `migrate`
- `migrate create` → `migrate new`

**Files:** `src/main.rs`

---

### UX-3: Write Flags Confusion

**Feedback:** "Need both --read-write AND --allow-write for INSERT operations - confusing"

**Analysis:** Two separate flags for writes is redundant for most use cases.

**Fix Options:**
1. Make `--allow-write` imply `--read-write`
2. Better error message explaining both are needed
3. Add `--write` as shorthand for both

**Recommendation:** Option 1 - `--allow-write` should imply `--read-write`

---

## Implementation Order

1. **BUG-1:** UTF-8 fix in sequences.rs (5 min)
2. **BUG-2:** xid_age type fix in triage.rs (5 min)
3. **BUG-3:** Empty database handling in xid.rs (15 min)
4. **BUG-4:** Triage sequences consistency (15 min)
5. **UX-1:** Verbose SQL logging (30 min)
6. **UX-2:** Command aliases (20 min)
7. **UX-3:** Write flag simplification (10 min)

**Total estimated:** ~2 hours

---

## Testing Plan

After fixes:
1. Rebuild pgcrate and install
2. Run builder-studio diagnostic tests:
   - `bloat-check`
   - `replication-check`
   - `vacuum-check`
   - `dba-full-diagnosis`
3. Run full pgcrate test suite in builder-studio
4. Collect new feedback and compare with before
