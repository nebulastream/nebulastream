/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
use nes_buffer_bindings::ffi::{MemorySegment, buffer_release};
use nes_buffer_bindings::{AllocatedSegment, OnBufferAllocated, ffi};

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

    /// Awaits a pooled buffer from the C++ getBufferAsync() coroutine. Returns the raw segment so
    /// the caller can run it through wrap_allocated_buffer and keep the ref-count accounting in one
    /// place.
    async fn allocate_async(&self) -> *mut MemorySegment {
        let (tx, rx) = tokio::sync::oneshot::channel::<AllocatedSegment>();
        unsafe {
            ffi::allocate_async(
                &self.handle,
                deliver_segment,
                Box::new(OnBufferAllocated(tx)),
            );
        }
        rx.await
            .expect("Buffer Manager was closed unexpectedly while awaiting a buffer. Which should not happen as we are holding a shared ptr")
            .0
    }

    pub async fn allocate(&self) -> TupleBuffer {
        // Fast path: grab a buffer without touching the async machinery if one is ready.
        let maybe_segment = ffi::try_allocate(&self.handle);
        if !maybe_segment.is_null() {
            return Self::wrap_allocated_buffer(maybe_segment);
        }
        // Slow path: park on the C++ coroutine until a buffer is recycled into the pool.
        Self::wrap_allocated_buffer(self.allocate_async().await)
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

/// Completion callback handed to `ffi::allocate_async`. Delivers the freshly allocated segment to
/// the awaiting `allocate()` future. If that future was dropped (allocation cancelled), the receiver
/// is gone and the send fails, in which case we release the buffer back to the pool so it is not
/// leaked.
fn deliver_segment(ctx: Box<OnBufferAllocated>, segment: *mut MemorySegment) {
    if let Err(AllocatedSegment(segment)) = ctx.0.send(AllocatedSegment(segment)) {
        unsafe { buffer_release(segment) }
    }
}
