// Buffer wrapper types for TupleBuffer and AbstractBufferProvider.
//
// These types provide safe Rust wrappers around the cxx bridge opaque types,
// managing reference counting (retain/release) and memory safety.
//
// In test builds, TupleBufferHandle uses a Vec<u8> scratch buffer instead of
// cxx::UniquePtr<TupleBuffer> to avoid requiring C++ linker symbols. This
// allows sources like GeneratorSource to exercise allocate_buffer()/emit()
// in pure Rust tests. Buffer contents are NOT verified in Rust tests -- that
// is the job of C++ integration tests.

use crate::TupleBuffer;
use nes_buffer_bindings::ffi::{buffer_release, MemorySegment};
use nes_buffer_bindings::{ffi, OnBufferAvailable};

pub struct BufferProvider {
    handle: cxx::SharedPtr<ffi::BufferProviderHandle>,
}
impl BufferProvider {
    pub fn from_raw(handle: cxx::SharedPtr<ffi::BufferProviderHandle>) -> BufferProvider {
        BufferProvider { handle }
    }

    fn wrap_allocated_buffer(segment: *mut MemorySegment) -> TupleBuffer {
        assert!(!segment.is_null());
        let buffer = unsafe { TupleBuffer::from_raw(segment) };
        unsafe { buffer_release(segment) }
        buffer
    }

    async fn wait_for_available_buffer(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        unsafe {
            ffi::wake_on_buffer_available(
                &self.handle,
                |ctx: Box<OnBufferAvailable>| ctx.0.send(()).is_ok(),
                Box::new(OnBufferAvailable(tx)),
            )
        }
        rx.await.expect("Buffer Manager was closed unexpectedly. Which should not happen as we are holding a shared ptr");
    }

    pub async fn allocate(&self) -> TupleBuffer {
        let mut maybe_segment = ffi::try_allocate(&self.handle);
        while maybe_segment.is_null() {
            self.wait_for_available_buffer().await;
            maybe_segment = ffi::try_allocate(&self.handle);
        }
        Self::wrap_allocated_buffer(maybe_segment)
    }
    pub fn try_allocate(&self) -> Option<TupleBuffer> {
        let maybe_segment = ffi::try_allocate(&self.handle);
        if maybe_segment.is_null() {
            return None;
        }
        Some(Self::wrap_allocated_buffer(maybe_segment))
    }

    /// Allocate an unpooled buffer of at least `size` bytes. Returns `None` if
    /// the allocation fails.
    pub fn try_allocate_unpooled(&self, size: usize) -> Option<TupleBuffer> {
        let maybe_segment = ffi::try_allocate_unpooled(&self.handle, size);
        if maybe_segment.is_null() {
            return None;
        }
        Some(Self::wrap_allocated_buffer(maybe_segment))
    }
}
