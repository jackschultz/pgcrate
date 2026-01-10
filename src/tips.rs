//! Contextual tips shown after commands to improve discoverability.
//!
//! Tips are shown:
//! - Only when stdout is a TTY (not in scripts/CI)
//! - Only when --quiet is not set
//! - One tip per command max
//! - Formatted subtly (dim text) so they don't look like command output

use std::io::{IsTerminal, Write};

/// Context for selecting an appropriate tip after a command.
pub enum TipContext {
    /// After successful model run
    RunSuccess { had_incremental: bool },
    /// After creating a new model
    New,
    /// After showing model status
    Status { missing: usize },
    /// After model move
    Move,
}

/// Show a contextual tip if appropriate.
///
/// Tips are suppressed if:
/// - quiet mode is enabled
/// - stdout is not a TTY
pub fn show_tip(ctx: TipContext, quiet: bool) {
    if quiet {
        return;
    }

    // Only show tips on interactive terminals
    if !std::io::stderr().is_terminal() {
        return;
    }

    let tip = select_tip(ctx);

    // Print to stderr so it doesn't interfere with piped output
    let mut stderr = std::io::stderr();
    // Use dim ANSI escape for subtle appearance
    let _ = writeln!(stderr, "\n\x1b[2mTip: {}\x1b[0m", tip);
}

/// Select the most relevant tip for the given context.
fn select_tip(ctx: TipContext) -> &'static str {
    match ctx {
        TipContext::RunSuccess { had_incremental } => {
            if had_incremental {
                "Verify with `pgcrate model status`; use `--full-refresh` to force complete rebuild"
            } else {
                "Verify with `pgcrate model status` to check sync state vs database"
            }
        }
        TipContext::New => {
            "`pgcrate model run -s <name>` runs just this model; `--dry-run` previews the plan"
        }
        TipContext::Status { missing } => {
            if missing > 0 {
                "`pgcrate model run` will create missing models"
            } else {
                "`pgcrate model run --dry-run` previews execution without changes"
            }
        }
        TipContext::Move => "`pgcrate model status` verifies the move succeeded",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tip_selection() {
        // Just verify tips are non-empty for each context
        let contexts = vec![
            TipContext::RunSuccess {
                had_incremental: false,
            },
            TipContext::RunSuccess {
                had_incremental: true,
            },
            TipContext::New,
            TipContext::Status { missing: 0 },
            TipContext::Status { missing: 2 },
            TipContext::Move,
        ];

        for ctx in contexts {
            let tip = select_tip(ctx);
            assert!(!tip.is_empty());
            assert!(tip.contains("pgcrate"));
        }
    }
}
