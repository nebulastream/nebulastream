// Tokio runtime management.
//
// Re-exports from nes_io_runtime which owns the OnceLock<Runtime> singleton.

pub use nes_io_runtime::init_io_runtime;
pub use nes_io_runtime::io_runtime;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_init_and_access() {
        let result = init_io_runtime(2);
        assert!(result.is_ok());
        let _rt = io_runtime();
    }

    #[test]
    fn runtime_double_init_no_panic() {
        let r1 = init_io_runtime(2);
        let r2 = init_io_runtime(4);
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }
}
