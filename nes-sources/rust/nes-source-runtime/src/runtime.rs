// Tokio runtime management for the source framework.
//
// Delegates to nes_buffer_bindings which owns the OnceLock<Runtime> singleton.
// This module re-exports init_source_runtime and source_runtime so that
// downstream code within the runtime crate can use them without depending
// on nes_buffer_bindings directly.

pub use nes_buffer_bindings::init_source_runtime;
pub use nes_buffer_bindings::source_runtime;

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: These tests share process-global OnceLock state and must run
    // with --test-threads=1 to avoid races. The first test that calls
    // init_source_runtime "wins" and sets the runtime; subsequent calls
    // are no-ops by design.

    #[test]
    fn runtime_init_and_access() {
        let result = init_source_runtime(2);
        assert!(result.is_ok());
        let _rt = source_runtime();
    }

    #[test]
    fn runtime_double_init_no_panic() {
        let r1 = init_source_runtime(2);
        let r2 = init_source_runtime(4);
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }
}
