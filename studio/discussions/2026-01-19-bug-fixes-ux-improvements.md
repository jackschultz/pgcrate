# Bug Fixes and UX Improvements from Agent Feedback

**Date:** 2026-01-19
**Source:** builder-studio agent feedback analysis (106 feedback issues)
**Status:** ✅ All bugs and UX improvements resolved

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

## Priority 2: UX Improvements (All Resolved)

### UX-1: --show-sql Flag for Triage ✅ FIXED

**Fixed in:** v0.4.0 (2026-01-19)

Added `pgcrate dba triage --show-sql` flag that prints all SQL queries used by triage.
This provides transparency into what queries are being run.

---

### UX-2: Command Naming Aliases ✅ ALREADY IMPLEMENTED

Aliases already existed:
- `pgcrate migration` → `pgcrate migrate` (visible_alias)
- `pgcrate migrate create` → `pgcrate migrate new` (visible_alias)

---

### UX-3: Write Flags ✅ ALREADY IMPLEMENTED

`--allow-write` already implies `--read-write` (line 1885-1886 in main.rs).

---

## Implementation Order (Historical)

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
