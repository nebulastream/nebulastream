use crate::ffi::MemorySegment;
use cxx::SharedPtr;
use nes_buffer_runtime::{BufferProvider, TupleBuffer};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;

#[cxx::bridge]
pub mod ffi {
    unsafe extern "C++" {
        include!("BufferBindings.hpp");
        #[namespace = "NES::detail"]
        type MemorySegment = nes_buffer_bindings::ffi::MemorySegment;
        type BufferProviderHandle = nes_buffer_bindings::ffi::BufferProviderHandle;
    }

    extern "Rust" {
        type TestCaseThing;
        unsafe fn create_test_case(
            buffer_provider: SharedPtr<BufferProviderHandle>,
        ) -> Box<TestCaseThing>;

        unsafe fn allocate_buffer_and_store(tc: &Box<TestCaseThing>) -> isize;
        unsafe fn release_buffer(tc: &Box<TestCaseThing>, handle: isize) -> *mut MemorySegment;
        unsafe fn store_buffer(tc: &Box<TestCaseThing>, segment: *mut MemorySegment) -> isize;
    }
}

struct TestCaseThing {
    runtime: tokio::runtime::Runtime,
    command: tokio::sync::mpsc::Sender<Command>,
}

enum Command {
    Store(TupleBuffer, oneshot::Sender<isize>),
    Allocate(oneshot::Sender<isize>),
    Release(isize, oneshot::Sender<TupleBuffer>),
}

unsafe fn create_test_case(
    buffer_provider: SharedPtr<crate::ffi::BufferProviderHandle>,
) -> Box<TestCaseThing> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let tc = Box::new(TestCaseThing {
        runtime: tokio::runtime::Runtime::new().unwrap(),
        command: tx,
    });

    tc.runtime.spawn(async move {
        let mut counter = 0u32;
        let mut buffers = HashMap::<u32, TupleBuffer>::default();
        let buffer_provider = BufferProvider::from_raw(buffer_provider);
        while let Some(command) = rx.recv().await {
            match command {
                Command::Allocate(tx) => {
                    let buffer =
                        tokio::time::timeout(Duration::from_secs(2), buffer_provider.allocate())
                            .await;
                    match buffer {
                        Ok(buffer) => {
                            buffers.insert(counter, buffer);
                            let _ = tx.send(counter as isize);
                            counter += 1;
                        }
                        Err(_) => {
                            let _ = tx.send(-1);
                        }
                    }
                }
                Command::Release(id, tx) => {
                    assert!(id >= 0);
                    if let Some(buffer) = buffers.remove(&(id as u32)) {
                        let _ = tx.send(buffer);
                    }
                }
                Command::Store(buffer, tx) => {
                    buffers.insert(counter, buffer);
                    let _ = tx.send(counter as isize);
                    counter += 1;
                }
            };
        }
    });

    return tc;
}

unsafe fn allocate_buffer_and_store(tc: &Box<TestCaseThing>) -> isize {
    let (tx, rx) = oneshot::channel();
    tc.command.blocking_send(Command::Allocate(tx)).unwrap();
    rx.blocking_recv().unwrap()
}
unsafe fn release_buffer(tc: &Box<TestCaseThing>, handle: isize) -> *mut MemorySegment {
    let (tx, rx) = oneshot::channel();
    tc.command
        .blocking_send(Command::Release(handle, tx))
        .unwrap();
    let buffer = rx.blocking_recv().unwrap();
    buffer.leak()
}

unsafe fn store_buffer(tc: &Box<TestCaseThing>, segment: *mut MemorySegment) -> isize {
    let buffer = unsafe { TupleBuffer::from_raw(segment) };
    let (tx, rx) = oneshot::channel();
    tc.command
        .blocking_send(Command::Store(buffer, tx))
        .unwrap();
    rx.blocking_recv().unwrap()
}
