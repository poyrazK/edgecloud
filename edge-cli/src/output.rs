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

/// Print a hint / "next step" suggestion in dim gray.
#[allow(dead_code)]
pub fn hint(msg: &str) {
    println!("{} {}", style("→").dim(), style(msg).dim());
}

/// Print a section header.
#[allow(dead_code)]
pub fn section(label: &str) {
    println!("\n{} {}", style("›").cyan(), style(label).bold());
}

/// Read a `y/N` confirmation. Returns true on "y" or "Y" (after
/// trim); false on anything else (including EOF and empty input).
/// Caller is responsible for the `is_terminal()` check.
///
/// On Unix we open `/dev/tty` directly so a piped stdin
/// (`cmd < /dev/null`, `yes | cmd`, heredoc) cannot silently
/// satisfy the prompt or trigger an immediate EOF that aborts
/// the action. Same pattern as `rpassword::prompt_password` for
/// `edge auth login --no-echo`. On non-Unix platforms we fall back
/// to stdin; the caller's `is_terminal()` gate is the only safety
/// on that path.
#[cfg(unix)]
pub(crate) fn confirm(prompt: &str) -> std::io::Result<bool> {
    use std::io::{BufRead, BufReader};
    eprint!("{prompt}");
    let tty = std::fs::File::open("/dev/tty")?;
    let mut buf = String::new();
    BufReader::new(tty).read_line(&mut buf)?;
    Ok(matches!(buf.trim(), "y" | "Y"))
}

#[cfg(not(unix))]
pub(crate) fn confirm(prompt: &str) -> std::io::Result<bool> {
    eprint!("{prompt}");
    let mut buf = String::new();
    std::io::stdin().read_line(&mut buf)?;
    Ok(matches!(buf.trim(), "y" | "Y"))
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
