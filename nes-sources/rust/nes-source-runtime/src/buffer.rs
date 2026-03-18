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

use std::pin::Pin;

use nes_io_bindings::ffi;

// ---- TupleBufferHandle: production build (wraps cxx::UniquePtr<TupleBuffer>) ----

/// Safe wrapper around cxx `UniquePtr<TupleBuffer>`.
///
/// Manages C++ reference counting: `Clone` calls `cloneTupleBuffer` (which
/// invokes the C++ copy constructor, incrementing the refcount via `retain`),
/// and `Drop` calls `release` to decrement the refcount.
#[cfg(not(test))]
pub struct TupleBufferHandle {
    inner: cxx::UniquePtr<ffi::TupleBuffer>,
}

// SAFETY: TupleBuffer's reference counting uses atomics (std::atomic<uint32_t>).
// Safe to send between threads. Not Sync because concurrent mutable access to
// the data region (raw byte buffer) is not synchronized.
#[cfg(not(test))]
unsafe impl Send for TupleBufferHandle {}

#[cfg(not(test))]
impl TupleBufferHandle {
    /// Create a new handle from a cxx UniquePtr received from C++.
    pub fn new(ptr: cxx::UniquePtr<ffi::TupleBuffer>) -> Self {
        Self { inner: ptr }
    }

    /// Get a mutable byte slice over the entire buffer capacity.
    ///
    /// # Safety guarantee
    /// The slice is valid for the lifetime of `&mut self`. The caller must
    /// not hold this slice across an await point or send it to another thread.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let cap = ffi::getCapacity(&*self.inner) as usize;
        let ptr = ffi::getDataPtrMut(self.inner.pin_mut());
        // SAFETY: getDataPtrMut returns a valid pointer to `cap` bytes.
        // The buffer memory is owned by the C++ TupleBuffer and valid for
        // as long as we hold the UniquePtr. We have &mut self so no aliasing.
        unsafe { std::slice::from_raw_parts_mut(ptr, cap) }
    }

    /// Get an immutable byte slice over the entire buffer capacity.
    pub fn as_slice(&self) -> &[u8] {
        let cap = ffi::getCapacity(&*self.inner) as usize;
        let ptr = ffi::getDataPtr(&*self.inner);
        // SAFETY: getDataPtr returns a valid pointer to `cap` bytes.
        // The buffer memory is valid for the lifetime of the UniquePtr.
        unsafe { std::slice::from_raw_parts(ptr, cap) }
    }

    /// Set the number of tuples written to this buffer.
    pub fn set_number_of_tuples(&mut self, count: u64) {
        ffi::setNumberOfTuples(self.inner.pin_mut(), count);
    }

    /// Get the number of tuples currently in this buffer.
    pub fn number_of_tuples(&self) -> u64 {
        ffi::getNumberOfTuples(&*self.inner)
    }

    /// Get the buffer capacity in bytes.
    pub fn capacity(&self) -> u64 {
        ffi::getCapacity(&*self.inner)
    }

    /// Get a raw pointer to the underlying TupleBuffer for FFI.
    ///
    /// This is a **borrowing** operation -- the `UniquePtr` still owns the
    /// buffer and `Drop` will still call `release()`.
    ///
    /// # Safety invariant
    /// The returned pointer is valid for the lifetime of `&mut self`. C++ must
    /// call `retain()` on this pointer before Rust drops the TupleBufferHandle
    /// (which calls `release()`). The `bridge_loop` guarantees this ordering:
    /// C++ `retain()` happens synchronously in the emit callback before the
    /// loop iteration ends and `msg.buffer` is dropped.
    pub(crate) fn as_raw_ptr(&mut self) -> *mut ffi::TupleBuffer {
        let pinned = self.inner.pin_mut();
        // SAFETY: We need a raw mutable pointer to pass through FFI. The
        // pointer is valid as long as `self` (and its UniquePtr) is alive.
        // The caller must ensure C++ retains the buffer before this handle
        // is dropped.
        unsafe { pinned.get_unchecked_mut() as *mut ffi::TupleBuffer }
    }
}

#[cfg(not(test))]
impl Clone for TupleBufferHandle {
    fn clone(&self) -> Self {
        // cloneTupleBuffer calls the C++ copy constructor, which calls retain
        // to increment the reference count.
        let cloned = ffi::cloneTupleBuffer(&*self.inner);
        Self { inner: cloned }
    }
}

#[cfg(not(test))]
impl Drop for TupleBufferHandle {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            ffi::release(self.inner.pin_mut());
        }
    }
}

// ---- TupleBufferHandle: test build (Vec<u8> scratch buffer, no C++ symbols) ----

/// Test-only TupleBufferHandle backed by a Vec<u8> scratch buffer.
///
/// Avoids C++ linker symbols entirely. Sources can call as_mut_slice(),
/// set_number_of_tuples(), emit() in pure Rust tests. Buffer contents are
/// NOT verified -- that is the job of C++ integration tests.
#[cfg(test)]
pub struct TupleBufferHandle {
    scratch: Vec<u8>,
    num_tuples: u64,
}

#[cfg(test)]
unsafe impl Send for TupleBufferHandle {}

#[cfg(test)]
impl TupleBufferHandle {
    /// Create a new handle from a cxx UniquePtr (production signature, kept for API compat).
    pub fn new(_ptr: cxx::UniquePtr<ffi::TupleBuffer>) -> Self {
        Self {
            scratch: vec![0u8; 4096],
            num_tuples: 0,
        }
    }

    /// Create a test-only handle with a scratch buffer.
    pub fn new_test() -> Self {
        Self {
            scratch: vec![0u8; 4096],
            num_tuples: 0,
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.scratch
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.scratch
    }

    pub fn set_number_of_tuples(&mut self, count: u64) {
        self.num_tuples = count;
    }

    pub fn number_of_tuples(&self) -> u64 {
        self.num_tuples
    }

    pub fn capacity(&self) -> u64 {
        self.scratch.len() as u64
    }

    pub(crate) fn as_raw_ptr(&mut self) -> *mut ffi::TupleBuffer {
        std::ptr::null_mut() as *mut ffi::TupleBuffer
    }
}

/// Handle to a C++ AbstractBufferProvider for allocating tuple buffers.
///
/// Stores a raw pointer because cxx cannot bridge `SharedPtr` for abstract C++
/// types. The C++ side maintains ownership via `shared_ptr`; the Rust side
/// borrows via raw pointer for the source's lifetime.
///
/// Does NOT implement `Clone` (one provider per source) or `Drop` (C++ owns
/// the provider's lifetime).
pub struct BufferProviderHandle {
    ptr: *mut ffi::AbstractBufferProvider,
}

// SAFETY: BufferManager (the concrete AbstractBufferProvider implementation)
// uses folly::MPMCQueue + atomics for thread-safe buffer allocation.
// Confirmed in RESEARCH.md.
unsafe impl Send for BufferProviderHandle {}

impl BufferProviderHandle {
    /// Create a handle from a raw pointer to a C++ AbstractBufferProvider.
    ///
    /// # Safety
    /// `ptr` must be valid for the lifetime of this handle. The caller (C++
    /// SourceHandle) must ensure the AbstractBufferProvider outlives all Rust
    /// sources using it.
    pub unsafe fn from_raw(ptr: *mut ffi::AbstractBufferProvider) -> Self {
        Self { ptr }
    }

    /// Allocate a buffer, blocking until one is available.
    ///
    /// This calls the C++ `getBufferBlocking()` which may block indefinitely
    /// waiting for a buffer to be recycled. Callers should use
    /// `tokio::task::spawn_blocking` to avoid starving async worker threads.
    #[cfg(not(test))]
    pub fn get_buffer_blocking(&self) -> TupleBufferHandle {
        // SAFETY: ptr is valid per from_raw contract.
        let provider_pin = unsafe { Pin::new_unchecked(&mut *self.ptr) };
        let buf = ffi::getBufferBlocking(provider_pin);
        TupleBufferHandle::new(buf)
    }

    /// Try to allocate a buffer without blocking.
    ///
    /// Returns `Some(handle)` if a buffer was available, `None` otherwise.
    /// This is non-blocking and safe to call from async contexts.
    #[cfg(not(test))]
    pub fn try_get_buffer(&self) -> Option<TupleBufferHandle> {
        let provider_pin = unsafe { Pin::new_unchecked(&mut *self.ptr) };
        let buf = ffi::tryGetBuffer(provider_pin);
        if buf.is_null() {
            None
        } else {
            Some(TupleBufferHandle::new(buf))
        }
    }

    /// Try to allocate a buffer (test build — always succeeds with test buffer).
    #[cfg(test)]
    pub fn try_get_buffer(&self) -> Option<TupleBufferHandle> {
        Some(TupleBufferHandle::new_test())
    }

    /// Get the buffer size in bytes for this provider.
    #[cfg(not(test))]
    pub fn buffer_size(&self) -> u64 {
        // SAFETY: ptr is valid per from_raw contract.
        let provider_ref = unsafe { &*self.ptr };
        ffi::getBufferSize(provider_ref)
    }

    /// Get the raw pointer for use in `spawn_blocking` closures.
    ///
    /// # Safety
    /// The raw pointer must not outlive the BufferProviderHandle's validity
    /// (i.e., the C++ AbstractBufferProvider's lifetime).
    pub(crate) fn as_raw_ptr(&self) -> *mut ffi::AbstractBufferProvider {
        self.ptr
    }
}

// ---- Async buffer notification ----

use std::sync::LazyLock;
use tokio::sync::Notify;

/// Global notification for async buffer allocation.
/// Signaled by C++ (via `on_buffer_recycled`) whenever a buffer is returned
/// to the pool. Async tasks waiting in `allocate_buffer` are woken.
pub(crate) static BUFFER_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Callback invoked by C++ BufferManager on every buffer recycle.
/// Exported with C linkage so C++ can reference it directly.
/// `notify_waiters()` is essentially free (atomic check) when nobody is waiting.
#[unsafe(no_mangle)]
pub extern "C" fn nes_on_buffer_recycled() {
    BUFFER_NOTIFY.notify_waiters();
}

/// Release one inflight slot on a Rust Arc<Semaphore>.
/// Called from C++ OnComplete callbacks when the pipeline finishes processing a buffer.
/// The ptr was created via Arc::into_raw() in emit() — this reconstructs and drops
/// the Arc (decrementing refcount) after adding a permit back to the semaphore.
#[unsafe(no_mangle)]
pub extern "C" fn nes_release_semaphore_slot(ptr: *const std::ffi::c_void) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let sem = std::sync::Arc::from_raw(ptr as *const tokio::sync::Semaphore);
        sem.add_permits(1);
        // Arc is dropped here, decrementing refcount
    }
}

/// Install the buffer recycle notification callback into the C++ BufferManager.
/// Called once during source runtime initialization.
#[cfg(not(test))]
pub fn install_buffer_notification() {
    ffi::installBufferRecycleNotification();
}

#[cfg(test)]
pub fn install_buffer_notification() {
    // No-op in test builds — no C++ BufferManager.
}

#[cfg(test)]
mod tests {
    use super::*;

    // Static assertion: TupleBufferHandle must be Send.
    fn _assert_send<T: Send>() {}

    #[test]
    fn tuple_buffer_handle_is_send() {
        _assert_send::<TupleBufferHandle>();
    }

    #[test]
    fn buffer_provider_handle_is_send() {
        _assert_send::<BufferProviderHandle>();
    }

    #[test]
    fn test_tuple_buffer_handle_api() {
        let mut buf = TupleBufferHandle::new_test();
        assert_eq!(buf.capacity(), 4096);
        assert_eq!(buf.number_of_tuples(), 0);

        buf.set_number_of_tuples(1);
        assert_eq!(buf.number_of_tuples(), 1);

        let slice = buf.as_mut_slice();
        assert!(slice.len() >= 8);
        slice[..8].copy_from_slice(&42u64.to_le_bytes());

        let rslice = buf.as_slice();
        assert_eq!(&rslice[..8], &42u64.to_le_bytes());
    }

    #[test]
    fn buffer_provider_handle_has_expected_api() {
        // Verify the type exists and has expected size (raw pointer)
        assert_eq!(
            std::mem::size_of::<BufferProviderHandle>(),
            std::mem::size_of::<*mut ffi::AbstractBufferProvider>()
        );
    }
}
