#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn rust_answer() -> i64;
    }
}

fn rust_answer() -> i64 {
    42
}
