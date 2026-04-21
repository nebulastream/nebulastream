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

    pub fn get_sequence_number(&self) -> u64 {
        unsafe { ffi::buffer_sequence_number(self.handle) }
    }

    pub fn get_origin_id(&self) -> u64 {
        unsafe { ffi::buffer_origin_id(self.handle) }
    }

    pub fn get_chunk_number(&self) -> u64 {
        unsafe { ffi::buffer_chunk_number(self.handle) }
    }

    pub fn get_watermark(&self) -> u64 {
        unsafe { ffi::buffer_watermark(self.handle) }
    }

    pub fn is_last_chunk(&self) -> bool {
        unsafe { ffi::buffer_last_chunk(self.handle) }
    }

    pub fn set_number_of_tuples(&mut self, n: usize) {
        unsafe { ffi::buffer_set_number_of_tuples(self.handle, n) }
    }
    pub fn set_sequence_number(&mut self, seq: u64) {
        unsafe { ffi::buffer_set_sequence_number(self.handle, seq) }
    }
    pub fn set_origin_id(&mut self, origin: u64) {
        unsafe { ffi::buffer_set_origin_id(self.handle, origin) }
    }
    pub fn set_chunk_number(&mut self, chunk: u64) {
        unsafe { ffi::buffer_set_chunk_number(self.handle, chunk) }
    }
    pub fn set_watermark(&mut self, watermark: u64) {
        unsafe { ffi::buffer_set_watermark(self.handle, watermark) }
    }
    pub fn set_last_chunk(&mut self, last_chunk: bool) {
        unsafe { ffi::buffer_set_last_chunk(self.handle, last_chunk) }
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
