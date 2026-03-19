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

#pragma once

#include <BufferBindings.hpp>

#include <Sources/SourceReturnType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <functional>
#include <stop_token>

namespace NES {

/// Context passed to bridge_emit as the opaque void* context pointer.
/// Holds the EmitFunction and a stop_source for generating stop tokens.
/// Owned by TokioSource; lifetime must exceed the Rust source task.
struct EmitContext {
    SourceReturnType::EmitFunction emitFunction;
    std::stop_source stopSource;
    uint64_t sequenceNumber{1}; // Must start at 1; SequenceNumber(0) is INVALID
    /// Set by bridge_emit after processing EOS (null buffer).
    /// Checked by TokioSource::tryStop() to know when it's safe to proceed.
    std::atomic<bool> eosProcessed{false};
};

} // namespace NES

extern "C" {
    /// Called by Rust bridge thread to emit a buffer into the C++ task queue.
    /// semaphore_ptr: Arc<Semaphore> raw pointer for inflight tracking (0 for EOS).
    /// Returns 0 (SUCCESS) or 1 (STOP_REQUESTED).
    uint8_t bridge_emit(
        void* context,        // EmitContext*
        uint64_t origin_id,
        void* buffer_ptr,     // TupleBuffer* (nullptr for EOS)
        uintptr_t semaphore_ptr  // Arc<Semaphore> raw pointer (0 for EOS)
    );

    /// Called by Rust monitoring task when a source errors or panics.
    void on_source_error_callback(
        void* context,        // ErrorContext*
        uint64_t source_id,
        const char* message
    );
}
