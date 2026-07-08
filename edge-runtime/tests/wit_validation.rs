//! WIT structural validation tests.
//!
//! These tests parse the vendored WIT files (`src/wit/` + `src/wit/deps/`)
//! using `wit-parser` and verify:
//!
//!   * The package metadata is correct (`edge:cloud@0.2.0`).
//!   * All worlds are present with the expected imports/exports.
//!   * Each `edge:cloud` interface has the expected function count
//!     (catches accidental additions or removals during upgrades).
//!   * The vendored WASI package references resolve.
//!
//! This is a structural integrity check — it runs fast (no compilation)
//! and protects against regressions from WIT edits, bindgen version
//! bumps, or dep changes that would otherwise surface as runtime errors.

use std::collections::HashMap;
use std::path::Path;
use wit_parser::{Resolve, WorldItem, WorldKey};

/// Parse the WIT directory (`src/wit/`) and return (Resolve, edge:cloud package id).
fn resolve_wit() -> (Resolve, wit_parser::PackageId) {
    let mut resolve = Resolve::new();
    let wit_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/wit");
    assert!(wit_dir.exists(), "WIT directory not found at {:?}", wit_dir);
    let (pkg_id, _files) = resolve
        .push_dir(&wit_dir)
        .expect("WIT package must parse and resolve without errors");
    (resolve, pkg_id)
}

/// Find a named world in the edge:cloud package.
fn find_world<'a>(
    resolve: &'a Resolve,
    pkg: wit_parser::PackageId,
    name: &str,
) -> &'a wit_parser::World {
    for (_id, world) in resolve.worlds.iter() {
        if world.name == name && world.package == Some(pkg) {
            return world;
        }
    }
    panic!("world '{name}' not found in edge:cloud package");
}

/// Collect edge:cloud interface imports from a world as (interface_name, function_count).
fn edge_interface_imports(resolve: &Resolve, world: &wit_parser::World) -> Vec<(String, usize)> {
    let mut result = Vec::new();
    for (_key, item) in &world.imports {
        if let WorldItem::Interface { id, .. } = item {
            if let Some(iface) = resolve.interfaces.get(*id) {
                // Check if this interface belongs to the edge:cloud package.
                if let Some(pkg) = iface.package {
                    let pkg_name = &resolve.packages[pkg].name;
                    if pkg_name.namespace == "edge" && pkg_name.name == "cloud" {
                        let name = iface
                            .name
                            .clone()
                            .unwrap_or_else(|| "<unnamed>".to_string());
                        result.push((name, iface.functions.len()));
                    }
                }
            }
        }
    }
    result
}

/// Collect all exports from a world as (full_name, function_count).
fn all_exports(resolve: &Resolve, world: &wit_parser::World) -> Vec<(String, usize)> {
    let mut result = Vec::new();
    for (key, item) in &world.exports {
        let key_str = match key {
            WorldKey::Name(n) => n.clone(),
            WorldKey::Interface(id) => {
                if let Some(iface) = resolve.interfaces.get(*id) {
                    iface
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("iface-{id:?}"))
                } else {
                    format!("<iface {id:?}>")
                }
            }
        };
        if let WorldItem::Interface { id, .. } = item {
            if let Some(iface) = resolve.interfaces.get(*id) {
                result.push((key_str, iface.functions.len()));
            }
        }
    }
    result
}

// ── Package metadata ────────────────────────────────────────────────────

#[test]
fn wit_package_parses() {
    let (resolve, pkg_id) = resolve_wit();
    let pkg = &resolve.packages[pkg_id];
    assert_eq!(pkg.name.namespace, "edge");
    assert_eq!(pkg.name.name, "cloud");
    assert_eq!(pkg.name.version, Some(semver::Version::new(0, 2, 0)));
}

#[test]
fn wit_package_has_two_edge_worlds() {
    let (resolve, pkg_id) = resolve_wit();
    let count = resolve
        .worlds
        .iter()
        .filter(|(_id, w)| w.package == Some(pkg_id))
        .count();
    assert_eq!(
        count, 2,
        "expected exactly 2 edge:cloud worlds (edge-runtime + edge-runtime-handler)"
    );
}

// ── World structure ─────────────────────────────────────────────────────

#[test]
fn edge_runtime_world_has_correct_imports() {
    let (resolve, pkg_id) = resolve_wit();
    let world = find_world(&resolve, pkg_id, "edge-runtime");

    let mut imports: HashMap<String, usize> = edge_interface_imports(&resolve, world)
        .into_iter()
        .collect();

    // Remove the auto-generated name or the explicit name — both appear
    // depending on how bindgen resolves the WIT package.
    assert_eq!(
        imports.remove("kv-store"),
        Some(9),
        "kv-store should have 9 functions"
    );
    assert_eq!(
        imports.remove("cache"),
        Some(10),
        "cache should have 10 functions"
    );
    assert_eq!(
        imports.remove("observe"),
        Some(5),
        "observe should have 5 functions"
    );
    assert_eq!(
        imports.remove("time"),
        Some(3),
        "time should have 3 functions"
    );
    assert_eq!(
        imports.remove("scheduling"),
        Some(3),
        "scheduling should have 3 functions"
    );
    assert_eq!(
        imports.remove("process"),
        Some(5),
        "process should have 5 functions"
    );
    assert_eq!(
        imports.remove("websocket"),
        Some(5),
        "websocket should have 5 functions"
    );

    // No unexpected interfaces.
    assert!(
        imports.is_empty(),
        "unexpected edge:cloud imports in edge-runtime world: {:?}",
        imports
    );
}

#[test]
fn edge_runtime_world_has_no_exports() {
    let (resolve, pkg_id) = resolve_wit();
    let world = find_world(&resolve, pkg_id, "edge-runtime");

    let exports = all_exports(&resolve, world);
    // The long-running world includes wasi:cli/command which exports
    // wasi:cli/run — that's a wasi export, not an edge:cloud export.
    // We check that there are no edge:cloud-prefixed exports.
    let edge_exports: Vec<_> = exports
        .iter()
        .filter(|(name, _)| name.starts_with("edge:"))
        .collect();
    assert!(
        edge_exports.is_empty(),
        "edge-runtime world should have no edge:cloud exports: {:?}",
        edge_exports
    );
}

#[test]
fn edge_runtime_handler_world_has_correct_imports() {
    let (resolve, pkg_id) = resolve_wit();
    let world = find_world(&resolve, pkg_id, "edge-runtime-handler");

    let mut imports: HashMap<String, usize> = edge_interface_imports(&resolve, world)
        .into_iter()
        .collect();

    assert_eq!(imports.remove("kv-store"), Some(9));
    assert_eq!(imports.remove("cache"), Some(10));
    assert_eq!(imports.remove("observe"), Some(5));
    assert_eq!(imports.remove("time"), Some(3));
    assert_eq!(imports.remove("scheduling"), Some(3));
    assert_eq!(imports.remove("process"), Some(5));
    assert_eq!(imports.remove("websocket"), Some(5));

    assert!(
        imports.is_empty(),
        "unexpected edge:cloud imports in handler world: {:?}",
        imports
    );
}

#[test]
fn edge_runtime_handler_world_exports_wasi_http_incoming_handler() {
    let (resolve, pkg_id) = resolve_wit();
    let world = find_world(&resolve, pkg_id, "edge-runtime-handler");

    let exports = all_exports(&resolve, world);
    let has_handler = exports.iter().any(|(name, _)| name == "incoming-handler");
    assert!(
        has_handler,
        "edge-runtime-handler world must export wasi:http/incoming-handler, got: {:?}",
        exports.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
    );
}

// ── Interface function counts ───────────────────────────────────────────

#[test]
fn interfaces_have_expected_functions() {
    let (resolve, _pkg_id) = resolve_wit();

    let expected: HashMap<&str, usize> = [
        ("kv-store", 9),
        ("cache", 10),
        ("observe", 5),
        ("time", 3),
        ("scheduling", 3),
        ("process", 5),
    ]
    .into();

    for (_id, iface) in resolve.interfaces.iter() {
        if let Some(name) = &iface.name {
            if let Some(&expected_count) = expected.get(name.as_str()) {
                assert_eq!(
                    iface.functions.len(),
                    expected_count,
                    "interface '{name}' has {} functions, expected {expected_count}",
                    iface.functions.len()
                );
            }
        }
    }
}

// ── Vendored WASI packages resolve ──────────────────────────────────────

#[test]
fn vendored_wasi_packages_resolve() {
    let (resolve, _pkg_id) = resolve_wit();
    let expected: [(&str, &str); 7] = [
        ("wasi", "cli"),
        ("wasi", "clocks"),
        ("wasi", "filesystem"),
        ("wasi", "http"),
        ("wasi", "io"),
        ("wasi", "random"),
        ("wasi", "sockets"),
    ];
    for (ns, name) in &expected {
        let found = resolve
            .packages
            .iter()
            .any(|(_id, p)| &p.name.namespace == ns && &p.name.name == name);
        assert!(
            found,
            "vendored WASI package '{ns}:{name}' not found in WIT resolve"
        );
    }
}

// ── Cross-world consistency ─────────────────────────────────────────────

#[test]
fn both_worlds_import_identical_edge_interfaces() {
    let (resolve, pkg_id) = resolve_wit();
    let world_a = find_world(&resolve, pkg_id, "edge-runtime");
    let world_b = find_world(&resolve, pkg_id, "edge-runtime-handler");

    let imports_a: HashMap<String, usize> = edge_interface_imports(&resolve, world_a)
        .into_iter()
        .collect();
    let imports_b: HashMap<String, usize> = edge_interface_imports(&resolve, world_b)
        .into_iter()
        .collect();

    assert_eq!(
        imports_a, imports_b,
        "both worlds must import the same edge:cloud interfaces with the same function counts"
    );
}
