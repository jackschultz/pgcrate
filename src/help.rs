use anyhow::Result;

/// Print LLM-specific help content
/// This embeds the llms.txt file at compile time, so it's always available
/// when the binary is distributed via crates.io
#[allow(dead_code)] // Used in tests
pub fn print_llm_help() -> Result<()> {
    const LLM_HELP: &str = include_str!("../llms.txt");
    print!("{}", LLM_HELP);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedded_llms_help() {
        // Test that the embedded content is not empty
        let llms_help = include_str!("../llms.txt");
        assert!(
            !llms_help.is_empty(),
            "Embedded llms.txt should not be empty"
        );
        assert!(
            llms_help.contains("pgcrate"),
            "Embedded llms.txt should mention pgcrate"
        );
        assert!(
            llms_help.contains("## OVERVIEW"),
            "Embedded llms.txt should have sections"
        );
    }

    #[test]
    fn test_print_llm_help() {
        // Just ensure it doesn't panic
        let result = print_llm_help();
        assert!(result.is_ok(), "print_llm_help should succeed");
    }
}
