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
//! All FFI lives in this crate. Without trace backends in the C++ build, every function is a no-op and counters are never
//! sampled.

#[cxx::bridge(namespace = "NES::EventTrace::Rust")]
mod ffi {
    /// The numeric values match `NES::EventTrace::Stage`.
    #[repr(u16)]
    enum Stage {
        SourceBackpressure = 0,
        SourceBufferWait = 1,
        SourceFill = 2,
        Enqueue = 3,
        Exec = 4,
        Emit = 5,
        NetSend = 6,
        NetRecv = 7,
    }

    /// Identifies a tuple buffer: origin id, sequence number and chunk number.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    struct Key {
        origin: u64,
        seq: u64,
        chunk: u32,
    }

    extern "Rust" {
        type Sample;
        fn sample(sample: &Sample) -> i64;
    }

    unsafe extern "C++" {
        include!("EventTraceBindings.hpp");

        type Counter;
        fn create_counter(name: &str, sample: Box<Sample>) -> UniquePtr<Counter>;

        fn now() -> u64;
        fn begin(stage: Stage, key: &Key, pipeline: u64);
        fn end(stage: Stage, key: &Key, pipeline: u64, extra: u64);
        fn mark(stage: Stage, key: &Key, pipeline: u64, time: u64);
    }
}

pub use ffi::{Key, Stage};

/// Steady-clock nanoseconds, the time base of all events.
pub fn now() -> u64 {
    ffi::now()
}

/// A span of `stage` starts now on the calling thread. See `EventTrace::begin` for how begins and ends pair up.
pub fn begin(stage: Stage, key: Key, pipeline: u64) {
    ffi::begin(stage, &key, pipeline);
}

/// The last span of `stage` that began on the calling thread ends now.
pub fn end(stage: Stage, key: Key, pipeline: u64, extra: u64) {
    ffi::end(stage, &key, pipeline, extra);
}

/// Something happened to the buffer `key` at `time` (see [`now`]).
pub fn mark(stage: Stage, key: Key, pipeline: u64, time: u64) {
    ffi::mark(stage, &key, pipeline, time);
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
