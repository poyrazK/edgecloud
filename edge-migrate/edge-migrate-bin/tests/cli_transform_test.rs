use std::process::Command;

#[test]
fn test_transform_flag_outputs_wasi_header() {
    let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/http_client.c");
    
    let output = Command::new(env!("CARGO_BIN_EXE_edge-migrate"))
        .arg("--transform")
        .arg(test_file)
        .output()
        .expect("failed to execute edge-migrate --transform");
    
    assert!(output.status.success(), "edge-migrate --transform failed: {}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);

    // WASI header must be present
    assert!(stdout.contains("#include <wasi/sockets.h>"), "WASI header missing");
    // WASI socket create must be present
    assert!(stdout.contains("wasi_socket_tcp_create"), "wasi_socket_tcp_create missing");
    // Original socket call must NOT be present (after header lines)
    // Use a longer, unambiguous pattern from the original call
    assert!(!stdout.contains("AF_INET, SOCK_STREAM, 0"), "original socket(AF_INET, SOCK_STREAM, 0) still present");
}
