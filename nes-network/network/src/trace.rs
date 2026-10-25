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

//! Steps of traced buffers through the network layer, recorded with the `event-trace` feature.

use crate::protocol::{OriginSequenceNumber, TupleBuffer};

#[cfg(feature = "event-trace")]
pub(crate) use event_trace::NetStep;

#[cfg(not(feature = "event-trace"))]
pub(crate) enum NetStep {
    Dequeue,
    Encoded,
    Acked,
    Decoded,
    Queued,
}

pub(crate) fn step(step: NetStep, buffer: &TupleBuffer) {
    record(step, buffer.trace_channel, buffer.sequence());
}

/// For buffers that were already handed on, by their `trace_channel` and sequence.
pub(crate) fn record(step: NetStep, trace_channel: u64, sequence: OriginSequenceNumber) {
    #[cfg(feature = "event-trace")]
    if trace_channel != 0 {
        event_trace::net_step(step, sequence, trace_channel);
    }
    #[cfg(not(feature = "event-trace"))]
    let _ = (step, trace_channel, sequence);
}
