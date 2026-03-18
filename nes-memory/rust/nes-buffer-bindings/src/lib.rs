#[cxx::bridge]
pub mod ffi {
    unsafe extern "C++" {
        include!("BufferBindings.hpp");

        type TupleBuffer;

        #[allow(non_snake_case)]
        fn retain(buf: Pin<&mut TupleBuffer>);
        #[allow(non_snake_case)]
        fn release(buf: Pin<&mut TupleBuffer>);
        #[allow(non_snake_case)]
        fn getDataPtr(buf: &TupleBuffer) -> *const u8;
        #[allow(non_snake_case)]
        fn getDataPtrMut(buf: Pin<&mut TupleBuffer>) -> *mut u8;
        #[allow(non_snake_case)]
        fn getCapacity(buf: &TupleBuffer) -> u64;
        #[allow(non_snake_case)]
        fn getNumberOfTuples(buf: &TupleBuffer) -> u64;
        #[allow(non_snake_case)]
        fn setNumberOfTuples(buf: Pin<&mut TupleBuffer>, count: u64);
        #[allow(non_snake_case)]
        fn getReferenceCounter(buf: &TupleBuffer) -> u32;
        #[allow(non_snake_case)]
        fn cloneTupleBuffer(buf: &TupleBuffer) -> UniquePtr<TupleBuffer>;

        // Child buffer access for variable-sized data (e.g. multi-segment output)
        #[allow(non_snake_case)]
        fn getNumberOfChildBuffers(buf: &TupleBuffer) -> u32;
        #[allow(non_snake_case)]
        fn loadChildBuffer(buf: &TupleBuffer, index: u32) -> UniquePtr<TupleBuffer>;

        type AbstractBufferProvider;

        #[allow(non_snake_case)]
        fn getBufferBlocking(provider: Pin<&mut AbstractBufferProvider>) -> UniquePtr<TupleBuffer>;
        #[allow(non_snake_case)]
        fn tryGetBuffer(provider: Pin<&mut AbstractBufferProvider>) -> UniquePtr<TupleBuffer>;
        #[allow(non_snake_case)]
        fn getBufferSize(provider: &AbstractBufferProvider) -> u64;
        #[allow(non_snake_case)]
        fn installBufferRecycleNotification();

        type BackpressureListener;

        #[allow(non_snake_case)]
        fn waitForBackpressure(listener: &BackpressureListener);
    }

}
