# Bug Fixes and UX Improvements from Agent Feedback

**Date:** 2026-01-19
**Source:** builder-studio agent feedback analysis (106 feedback issues)

---

## Priority 1: Bugs

### BUG-1: UTF-8 String Slicing in sequences.rs

**Location:** `src/commands/sequences.rs:181-184`

**Problem:**
```rust
if full_name.len() > 40 {
    format!("{}...", &full_name[..37])
```
This will panic on multi-byte UTF-8 characters (same bug we fixed in bloat.rs).

**Fix:**
```rust
if full_name.chars().count() > 40 {
    format!("{}...", full_name.chars().take(37).collect::<String>())
```

**Impact:** Crash prevention for non-ASCII schema/sequence names.

---

### BUG-2: xid_age Type Overflow in triage.rs

**Location:** `src/commands/triage.rs:334`

**Problem:**
```rust
let xid_age: i32 = row.get("xid_age");
```
The `age()` function can return values > 2^31 for very old databases. The xid.rs command uses i64, but triage uses i32.

**Fix:** Change to i64:
```rust
let xid_age: i64 = row.get("xid_age");
```

And update the threshold comparisons accordingly.

**Impact:** Prevents overflow on databases with high XID age.

---

### BUG-3: Empty Database Handling in xid Command

**Location:** `src/commands/xid.rs:110-123`

**Problem:** Query on `pg_stat_user_tables JOIN pg_class` returns "column relfrozenxid does not exist" error on empty databases or when table query fails.

**Analysis:** The error message is misleading. The real issue is likely:
1. Query fails when no user tables exist
2. Error handling doesn't catch this gracefully

**Fix:** Add explicit handling for empty result sets and improve error messages:
```rust
let rows = client.query(query, &[&(limit as i64)]).await?;
// If no tables, return empty vec (not an error)
if rows.is_empty() {
    return Ok(vec![]);
}
```

---

### BUG-4: Triage Sequences Display Inconsistency

**Location:** `src/commands/triage.rs:385-471`

**Problem:** After fixing a sequence, triage still shows CRITICAL but detailed `sequences` command shows healthy.

**Analysis:** The triage `check_sequences` function uses a different query than the full `sequences` command. The triage query calculates percentage on-the-fly while sequences command may have different rounding.

**Fix:** Ensure both use consistent percentage calculation and rounding.

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
