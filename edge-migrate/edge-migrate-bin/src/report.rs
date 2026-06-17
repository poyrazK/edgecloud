//! CLI report formatting.
//!
//! Formats migration reports for terminal display.

use edge_migrate_lib::report::{MigrationReport, MigrationStatus};

/// Print the local analysis report (before upload).
pub fn print_analysis_report(report: &MigrationReport) {
    println!();
    println!("=== edge-migrate Analysis Report ===");
    println!();
    println!("App name: {}", report.app_name);
    println!();

    if report.patterns_detected.is_empty() {
        println!("No POSIX patterns detected.");
        return;
    }

    println!("Patterns detected: {}", report.patterns_detected.len());
    println!();

    if !report.patterns_transformed.is_empty() {
        println!(
            "Auto-transformable ({}):",
            report.patterns_transformed.len()
        );
        for p in &report.patterns_transformed {
            println!(
                "  ✅ Line {}: {} → {}",
                p.line, p.pattern, p.wasi_equivalent
            );
        }
        println!();
    }

    if !report.patterns_manual_review.is_empty() {
        println!(
            "Manual review required ({}):",
            report.patterns_manual_review.len()
        );
        for p in &report.patterns_manual_review {
            println!("  ⚠️  Line {}: {}", p.line, p.pattern);
            println!("      WASI equivalent: {}", p.wasi_equivalent);
        }
        println!();
    }
}

/// Print the server response report (after upload).
pub fn print_server_report(report: &MigrationReport) {
    println!();
    match report.status {
        MigrationStatus::Success => {
            println!("✅ Migration successful!");
            println!();
            println!(
                "Binary stored. Run `edge deploy {} --id {}` to go live.",
                report.app_name,
                report.deployment_id.as_deref().unwrap_or("<id>")
            );
        }
        MigrationStatus::Partial => {
            println!("⚠️  Migration partially successful.");
            println!();
            println!("Binary stored, but some patterns require manual review:");
            for p in &report.patterns_manual_review {
                println!(
                    "  ⚠️  Line {}: {} — {}",
                    p.line, p.pattern, p.wasi_equivalent
                );
            }
            println!();
            println!(
                "Run `edge deploy {} --id {}` to deploy anyway.",
                report.app_name,
                report.deployment_id.as_deref().unwrap_or("<id>")
            );
        }
        MigrationStatus::Failed => {
            println!("❌ Migration failed.");
            println!();
            println!("The following patterns could not be auto-transformed:");
            for p in &report.patterns_manual_review {
                println!(
                    "  ❌ Line {}: {} — {}",
                    p.line, p.pattern, p.wasi_equivalent
                );
            }
            println!();
            println!("Fix these issues and re-run `edge-migrate`.");
        }
    }

    if !report.errors.is_empty() {
        println!();
        println!("Errors:");
        for e in &report.errors {
            println!("  ❌ Line {}: {}", e.line, e.message);
        }
    }
}
