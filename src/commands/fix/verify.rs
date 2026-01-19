//! Verification runner for fix commands.
//!
//! After a fix is executed, verification steps can be run to confirm the fix worked.
//! Each step specifies a command to run, and an expected result.

use anyhow::{Context, Result};
use std::process::Command;

use super::common::{VerificationResult, VerifyStep, VerifyStepResult};

/// Run verification steps and return results.
///
/// This executes each verification step by running the specified command
/// and checking if the output matches the expected condition.
pub fn run_verification(steps: &[VerifyStep]) -> VerificationResult {
    let mut step_results = Vec::new();
    let mut all_passed = true;

    for step in steps {
        let result = run_single_step(step);
        if !result.passed {
            all_passed = false;
        }
        step_results.push(result);
    }

    VerificationResult {
        passed: all_passed,
        steps: step_results,
    }
}

/// Run a single verification step.
fn run_single_step(step: &VerifyStep) -> VerifyStepResult {
    // Parse the command
    let parts: Vec<&str> = step.command.split_whitespace().collect();
    if parts.is_empty() {
        return VerifyStepResult {
            description: step.description.clone(),
            passed: false,
            actual: None,
            error: Some("Empty command".to_string()),
        };
    }

    let program = parts[0];
    let args = &parts[1..];

    // Execute the command
    let output = match Command::new(program).args(args).output() {
        Ok(o) => o,
        Err(e) => {
            return VerifyStepResult {
                description: step.description.clone(),
                passed: false,
                actual: None,
                error: Some(format!("Failed to execute command: {}", e)),
            };
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return VerifyStepResult {
            description: step.description.clone(),
            passed: false,
            actual: None,
            error: Some(format!("Command failed: {}", stderr)),
        };
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse JSON output
    let json: serde_json::Value = match serde_json::from_str(&stdout) {
        Ok(v) => v,
        Err(e) => {
            return VerifyStepResult {
                description: step.description.clone(),
                passed: false,
                actual: Some(stdout.to_string()),
                error: Some(format!("Failed to parse JSON: {}", e)),
            };
        }
    };

    // Evaluate the expected condition
    match evaluate_condition(&json, &step.expected) {
        Ok(passed) => VerifyStepResult {
            description: step.description.clone(),
            passed,
            actual: Some(format_actual(&json, &step.expected)),
            error: if passed {
                None
            } else {
                Some(format!("Condition not met: {}", step.expected))
            },
        },
        Err(e) => VerifyStepResult {
            description: step.description.clone(),
            passed: false,
            actual: Some(stdout.to_string()),
            error: Some(format!("Failed to evaluate condition: {}", e)),
        },
    }
}

/// Find the outer comparison operator in a condition string.
/// Returns the operator and its position, skipping operators inside brackets.
fn find_outer_operator(s: &str) -> (Option<&'static str>, usize) {
    let mut depth = 0;
    let chars: Vec<char> = s.chars().collect();

    for i in 0..chars.len() {
        match chars[i] {
            '[' | '(' => depth += 1,
            ']' | ')' => depth -= 1,
            '!' if depth == 0 && i + 1 < chars.len() && chars[i + 1] == '=' => {
                return (Some("!="), i);
            }
            '=' if depth == 0 && i + 1 < chars.len() && chars[i + 1] == '=' => {
                return (Some("=="), i);
            }
            _ => {}
        }
    }

    (None, 0)
}

/// Evaluate a simplified JSONPath-like condition.
///
/// Supports basic patterns like:
/// - `$.data.field` - check field exists and is truthy
/// - `$.data.field == "value"` - equality check
/// - `$.data.field != "value"` - inequality check
/// - `$.data.array.length() == 0` - array length check
/// - `$.data.status != 'critical'` - status check
fn evaluate_condition(json: &serde_json::Value, condition: &str) -> Result<bool> {
    // Parse the condition
    let condition = condition.trim();

    // Find the outer comparison operator (not inside brackets)
    let (op, split_pos) = find_outer_operator(condition);

    match op {
        Some("==") => {
            let path = condition[..split_pos].trim();
            let expected = condition[split_pos + 2..]
                .trim()
                .trim_matches(|c| c == '\'' || c == '"');

            // Handle .length() == N
            if let Some(array_path) = path.strip_suffix(".length()") {
                let value = get_json_path(json, array_path)?;
                if let Some(arr) = value.as_array() {
                    let expected_len: usize = expected.parse().context("Invalid length")?;
                    return Ok(arr.len() == expected_len);
                }
                return Ok(false);
            }

            let value = get_json_path(json, path)?;
            Ok(value_matches(&value, expected))
        }
        Some("!=") => {
            let path = condition[..split_pos].trim();
            let expected = condition[split_pos + 2..]
                .trim()
                .trim_matches(|c| c == '\'' || c == '"');

            // Handle .length() != N
            if let Some(array_path) = path.strip_suffix(".length()") {
                let value = get_json_path(json, array_path)?;
                if let Some(arr) = value.as_array() {
                    let expected_len: usize = expected.parse().context("Invalid length")?;
                    return Ok(arr.len() != expected_len);
                }
                return Ok(true); // Not an array, so != holds
            }

            let value = get_json_path(json, path)?;
            Ok(!value_matches(&value, expected))
        }
        _ => {
            // Simple existence/truthy check
            let value = get_json_path(json, condition)?;
            Ok(!value.is_null() && value != serde_json::Value::Bool(false))
        }
    }
}

/// Get a value from JSON using a simplified JSONPath.
///
/// Supports:
/// - `$.foo.bar` - object traversal
/// - `$.foo[0]` - array index
/// - `$.foo[?(@.field=='value')]` - simple array filter (first match)
fn get_json_path(json: &serde_json::Value, path: &str) -> Result<serde_json::Value> {
    let path = path.trim_start_matches("$.");
    let path = path.trim_start_matches('$');

    let mut current = json.clone();

    for part in split_path(path) {
        if part.is_empty() {
            continue;
        }

        // Check for array filter: [?(@.field=='value')]
        if part.starts_with("[?(") && part.ends_with(")]") {
            let filter = &part[3..part.len() - 2]; // Extract @.field=='value'
            current = apply_array_filter(&current, filter)?;
            continue;
        }

        // Check for array index only: [0]
        if part.starts_with('[') && part.ends_with(']') {
            let idx_str = &part[1..part.len() - 1];
            let idx: usize = idx_str.parse().context("Invalid array index")?;
            if let Some(arr) = current.as_array() {
                current = arr.get(idx).cloned().unwrap_or(serde_json::Value::Null);
            } else {
                return Ok(serde_json::Value::Null);
            }
            continue;
        }

        // Check for field[index] pattern (e.g., items[0])
        if let Some(bracket_idx) = part.find('[') {
            let field_name = &part[..bracket_idx];
            let bracket_part = &part[bracket_idx..];

            // First access the field
            if let Some(obj) = current.as_object() {
                current = obj
                    .get(field_name)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
            } else {
                return Ok(serde_json::Value::Null);
            }

            // Then apply the bracket operation
            if bracket_part.starts_with("[?(") && bracket_part.ends_with(")]") {
                let filter = &bracket_part[3..bracket_part.len() - 2];
                current = apply_array_filter(&current, filter)?;
            } else if bracket_part.starts_with('[') && bracket_part.ends_with(']') {
                let idx_str = &bracket_part[1..bracket_part.len() - 1];
                let idx: usize = idx_str.parse().context("Invalid array index")?;
                if let Some(arr) = current.as_array() {
                    current = arr.get(idx).cloned().unwrap_or(serde_json::Value::Null);
                } else {
                    return Ok(serde_json::Value::Null);
                }
            }
            continue;
        }

        // Object field access
        if let Some(obj) = current.as_object() {
            current = obj.get(part).cloned().unwrap_or(serde_json::Value::Null);
        } else {
            return Ok(serde_json::Value::Null);
        }
    }

    Ok(current)
}

/// Split a path by dots, respecting brackets.
fn split_path(path: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0;

    for (i, c) in path.char_indices() {
        match c {
            '[' => depth += 1,
            ']' => depth -= 1,
            '.' if depth == 0 => {
                if start < i {
                    parts.push(&path[start..i]);
                }
                start = i + 1;
            }
            _ => {}
        }
    }

    if start < path.len() {
        parts.push(&path[start..]);
    }

    parts
}

/// Apply a simple array filter like @.field=='value'
fn apply_array_filter(json: &serde_json::Value, filter: &str) -> Result<serde_json::Value> {
    let arr = json.as_array().context("Expected array for filter")?;

    // Parse filter: @.field=='value' or @.field=='value' && @.other=='val2'
    // For simplicity, we only support single conditions with == for now
    let filter = filter.trim_start_matches('@');

    if let Some((field_path, value)) = filter.split_once("==") {
        let field_path = field_path.trim().trim_start_matches('.');
        let value = value.trim().trim_matches(|c| c == '\'' || c == '"');

        for item in arr {
            let item_value = get_json_path(item, field_path)?;
            if value_matches(&item_value, value) {
                return Ok(item.clone());
            }
        }
    }

    Ok(serde_json::Value::Null)
}

/// Check if a JSON value matches an expected string.
fn value_matches(value: &serde_json::Value, expected: &str) -> bool {
    match value {
        serde_json::Value::String(s) => s == expected,
        serde_json::Value::Number(n) => n.to_string() == expected,
        serde_json::Value::Bool(b) => (*b && expected == "true") || (!*b && expected == "false"),
        serde_json::Value::Null => expected == "null",
        _ => false,
    }
}

/// Format the actual value for display based on the condition type.
fn format_actual(json: &serde_json::Value, condition: &str) -> String {
    // Try to extract the relevant part of the JSON for display
    if let Some((path, _)) = condition
        .split_once("==")
        .or_else(|| condition.split_once("!="))
    {
        let path = path.trim();
        if let Ok(value) = get_json_path(json, path) {
            return serde_json::to_string_pretty(&value).unwrap_or_else(|_| "?".to_string());
        }
    }
    serde_json::to_string_pretty(json).unwrap_or_else(|_| "Failed to format".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_equality() {
        let json: serde_json::Value = serde_json::json!({
            "data": {
                "status": "healthy",
                "count": 5
            }
        });

        assert!(evaluate_condition(&json, "$.data.status == 'healthy'").unwrap());
        assert!(!evaluate_condition(&json, "$.data.status == 'critical'").unwrap());
        assert!(evaluate_condition(&json, "$.data.count == '5'").unwrap());
    }

    #[test]
    fn test_evaluate_inequality() {
        let json: serde_json::Value = serde_json::json!({
            "data": {
                "status": "healthy"
            }
        });

        assert!(evaluate_condition(&json, "$.data.status != 'critical'").unwrap());
        assert!(!evaluate_condition(&json, "$.data.status != 'healthy'").unwrap());
    }

    #[test]
    fn test_evaluate_array_length() {
        let json: serde_json::Value = serde_json::json!({
            "data": {
                "items": [1, 2, 3],
                "empty": []
            }
        });

        assert!(evaluate_condition(&json, "$.data.items.length() == 3").unwrap());
        assert!(evaluate_condition(&json, "$.data.empty.length() == 0").unwrap());
        assert!(evaluate_condition(&json, "$.data.items.length() != 0").unwrap());
    }

    #[test]
    fn test_evaluate_array_filter() {
        let json: serde_json::Value = serde_json::json!({
            "data": {
                "sequences": [
                    {"name": "seq1", "status": "healthy"},
                    {"name": "seq2", "status": "critical"}
                ]
            }
        });

        // Check that a filtered sequence has the expected status
        assert!(evaluate_condition(
            &json,
            "$.data.sequences[?(@.name=='seq1')].status == 'healthy'"
        )
        .unwrap());

        assert!(evaluate_condition(
            &json,
            "$.data.sequences[?(@.name=='seq2')].status != 'healthy'"
        )
        .unwrap());
    }

    #[test]
    fn test_split_path() {
        assert_eq!(split_path("foo.bar.baz"), vec!["foo", "bar", "baz"]);
        assert_eq!(split_path("foo[0].bar"), vec!["foo[0]", "bar"]);
        assert_eq!(
            split_path("foo[?(@.x=='y')].bar"),
            vec!["foo[?(@.x=='y')]", "bar"]
        );
    }

    #[test]
    fn test_get_json_path() {
        let json: serde_json::Value = serde_json::json!({
            "data": {
                "items": [
                    {"name": "a"},
                    {"name": "b"}
                ]
            }
        });

        let result = get_json_path(&json, "$.data.items[0].name").unwrap();
        assert_eq!(result, serde_json::Value::String("a".to_string()));

        let result = get_json_path(&json, "data.items[1].name").unwrap();
        assert_eq!(result, serde_json::Value::String("b".to_string()));
    }
}
