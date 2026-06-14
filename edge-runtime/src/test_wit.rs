#![allow(dead_code)]
wasmtime::component::bindgen!({
    world: "test-world",
    inline: "
package test:mine;
interface foo {
    record request { name: string }
    do-stuff: func(req: request) -> string;
}
world test-world {
    export foo;
}
",
});

pub fn test() {
    let _e = test_world::Edge;
    let _f = test_world::add_to_linker;
}
