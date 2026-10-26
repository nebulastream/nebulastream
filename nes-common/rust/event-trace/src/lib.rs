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

//! Safe Rust API for the NebulaStream event trace (nes-common/include/Util/EventTrace.hpp).
//!
//! All FFI lives in this crate. Without trace backends in the C++ build, counters are never sampled and events are dropped.

#[cxx::bridge(namespace = "NES::EventTrace::Rust")]
mod ffi {
    /// A step of a buffer through the network layer, see the Net* event types in EventTrace.hpp.
    #[repr(u8)]
    enum NetStep {
        Dequeue,
        Encoded,
        Acked,
        Decoded,
        Queued,
    }

    extern "Rust" {
        type Sample;
        fn sample(sample: &Sample) -> i64;
    }

    unsafe extern "C++" {
        include!("EventTraceBindings.hpp");

        type Counter;
        fn create_counter(name: &str, sample: Box<Sample>) -> UniquePtr<Counter>;
        fn net_step(step: NetStep, origin: u64, seq: u64, chunk: u64);
    }
}

pub use ffi::NetStep;

/// Records that the buffer (origin, sequence number, chunk number) reached `step` in the network layer.
pub fn net_step(step: NetStep, (origin, seq, chunk): (u64, u64, u64)) {
    ffi::net_step(step, origin, seq, chunk);
}

struct Sample(Box<dyn Fn() -> i64 + Send + Sync>);

fn sample(sample: &Sample) -> i64 {
    (sample.0)()
}

/// The C++ counter is only created and destroyed through this crate, and its sampling is synchronized by the C++ sampler.
unsafe impl Send for ffi::Counter {}
unsafe impl Sync for ffi::Counter {}

/// Samples `sample` every millisecond on the event trace's sampler thread while this value is alive, like
/// `EventTrace::CounterUpdater`. Dropping it waits for a running sample to finish. `sample` must be cheap and must not panic
/// or block, because it runs on the sampler thread with the sampler's lock held.
pub struct CounterUpdater {
    _counter: cxx::UniquePtr<ffi::Counter>,
}

impl CounterUpdater {
    pub fn new(name: &str, sample: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        CounterUpdater {
            _counter: ffi::create_counter(name, Box::new(Sample(Box::new(sample)))),
        }
    }
}
