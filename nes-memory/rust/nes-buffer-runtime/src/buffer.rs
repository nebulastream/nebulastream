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

use nes_buffer_bindings::ffi;
use nes_buffer_bindings::ffi::{buffer_release, buffer_retain, MemorySegment};
use std::mem;
use std::ptr::NonNull;

pub struct TupleBuffer {
    handle: *mut MemorySegment,
    size: usize,
    data_ptr: NonNull<u8>,
}

/// Safety: TupleBuffer are thread safe. The content is not but the handle itself is.
unsafe impl Send for TupleBuffer {}
unsafe impl Sync for TupleBuffer {}
pub struct ChildBufferIndex(pub usize);
impl TupleBuffer {
    pub unsafe fn from_raw(handle: *mut MemorySegment) -> TupleBuffer {
        buffer_retain(handle);
        TupleBuffer {
            handle,
            size: ffi::buffer_size(handle),
            data_ptr: NonNull::from_mut(&mut *ffi::buffer_data(handle)),
        }
    }
    pub fn get_data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data_ptr.as_ptr(), self.size) }
    }
    pub fn get_data_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr.as_ptr(), self.size) }
    }

    pub fn load_child(&self, index: ChildBufferIndex) -> TupleBuffer {
        unsafe { TupleBuffer::from_raw(ffi::buffer_load_child(self.handle, index.0)) }
    }

    pub fn store_child(&mut self, child_buffer: &TupleBuffer) -> ChildBufferIndex {
        ChildBufferIndex(unsafe { ffi::buffer_store_child(self.handle, child_buffer.handle) })
    }

    pub fn get_number_of_tuples(&self) -> usize {
        unsafe { ffi::buffer_number_of_tuples(self.handle) }
    }

    pub fn num_children(&self) -> usize {
        unsafe { ffi::buffer_num_children(self.handle) }
    }

    pub fn leak(self) -> *mut MemorySegment {
        let handle = self.handle;
        mem::forget(self);
        handle
    }
}

impl Clone for TupleBuffer {
    fn clone(&self) -> Self {
        // SAFETY: Assuming the TupleBuffer has been created from a valid handle via from_raw cloning
        // does not change the validity of the handle it just needs to increase the ref count
        unsafe { buffer_retain(self.handle) }
        TupleBuffer {
            handle: self.handle,
            size: self.size,
            data_ptr: self.data_ptr,
        }
    }
}

impl Drop for TupleBuffer {
    fn drop(&mut self) {
        // SAFETY: Assuming the TupleBuffer has been created from a valid handle via from_raw the
        // ref count has been increased and thus needs to be released on drop.
        unsafe { buffer_release(self.handle) }
    }
}
