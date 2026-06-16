//! Styled terminal output.

use console::style;

/// Print a success message in green.
#[allow(dead_code)]
pub fn success(msg: &str) {
    println!("{}", style(msg).green());
}

/// Print an error message in red.
#[allow(dead_code)]
pub fn error(msg: &str) {
    eprintln!("{}", style(msg).red());
}

/// Print a warning message in yellow.
#[allow(dead_code)]
pub fn warn(msg: &str) {
    eprintln!("{}", style(msg).yellow());
}

/// Print an info message in cyan.
#[allow(dead_code)]
pub fn info(msg: &str) {
    println!("{}", style(msg).cyan());
}

/// Print a section header.
#[allow(dead_code)]
pub fn section(label: &str) {
    println!("\n{} {}", style("›").cyan(), style(label).bold());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success_does_not_panic() {
        success("Deployment successful!");
    }

    #[test]
    fn test_success_accepts_empty_string() {
        success("");
    }

    #[test]
    fn test_error_does_not_panic() {
        error("Something went wrong");
    }

    #[test]
    fn test_error_accepts_empty_string() {
        error("");
    }

    #[test]
    fn test_warn_does_not_panic() {
        warn("This is a warning");
    }

    #[test]
    fn test_warn_accepts_empty_string() {
        warn("");
    }

    #[test]
    fn test_info_does_not_panic() {
        info("Info message");
    }

    #[test]
    fn test_info_accepts_empty_string() {
        info("");
    }

    #[test]
    fn test_section_does_not_panic() {
        section("Configuration");
    }

    #[test]
    fn test_section_accepts_empty_string() {
        section("");
    }

    #[test]
    fn test_section_with_long_string() {
        section("This is a very long section header that should still work");
    }

    #[test]
    fn test_error_with_multiline_message() {
        error("Line 1\nLine 2\nLine 3");
    }
}
