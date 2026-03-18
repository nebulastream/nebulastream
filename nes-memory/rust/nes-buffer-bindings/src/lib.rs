#[cxx::bridge]
pub mod ffi {
    unsafe extern "C++" {
        include!("BufferBindings.hpp");
        #[namespace = "NES::detail"]
        type MemorySegment;
        #[namespace = "NES::detail"]
        unsafe fn buffer_retain(handle: *mut MemorySegment);
        #[namespace = "NES::detail"]
        unsafe fn buffer_release(handle: *mut MemorySegment);
        #[namespace = "NES::detail"]
        unsafe fn buffer_size(handle: *mut MemorySegment) -> usize;
        #[namespace = "NES::detail"]
        unsafe fn buffer_data(handle: *mut MemorySegment) -> *mut u8;
        #[namespace = "NES::detail"]
        unsafe fn buffer_load_child(handle: *mut MemorySegment, index: usize)
        -> *mut MemorySegment;
        #[namespace = "NES::detail"]
        unsafe fn buffer_store_child(
            handle: *mut MemorySegment,
            child: *mut MemorySegment,
        ) -> usize;
        #[namespace = "NES::detail"]
        unsafe fn buffer_num_children(handle: *mut MemorySegment) -> usize;
        #[namespace = "NES::detail"]
        unsafe fn buffer_number_of_tuples(handle: *mut MemorySegment) -> usize;
    }

    extern "Rust" {
        type OnBufferAvailable;
    }

    unsafe extern "C++" {
        include!("BufferBindings.hpp");
        type BufferProviderHandle;
        fn try_allocate(handle: &SharedPtr<BufferProviderHandle>) -> *mut MemorySegment;
        unsafe fn wake_on_buffer_available(
            handle: &SharedPtr<BufferProviderHandle>,
            done: fn(Box<OnBufferAvailable>) -> bool,
            ctx: Box<OnBufferAvailable>,
        );
    }
}
pub struct OnBufferAvailable(pub tokio::sync::oneshot::Sender<()>);

unsafe impl Send for ffi::MemorySegment {}
unsafe impl Sync for ffi::MemorySegment {}

unsafe impl Send for ffi::BufferProviderHandle {}
unsafe impl Sync for ffi::BufferProviderHandle {}
