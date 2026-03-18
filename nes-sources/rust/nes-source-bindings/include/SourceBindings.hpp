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
///
/// Ref-counted via shared_ptr: TokioSource holds one shared_ptr, and each
/// BridgeMessage on the Rust side holds another (via emit_context_clone /
/// emit_context_drop). The context is freed when the last reference is
/// released — no use-after-free on query restart.
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
        void* context,        // EmitContext*  (raw ptr from shared_ptr::get())
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

    /// Clone a shared_ptr<EmitContext> handle.
    /// Takes an opaque pointer to a heap-allocated shared_ptr<EmitContext>,
    /// creates a new heap-allocated copy (incrementing refcount), returns it.
    /// Called by Rust when creating EmitCallback copies for BridgeMessages.
    void* emit_context_clone(void* shared_ptr_handle);

    /// Drop a shared_ptr<EmitContext> handle.
    /// Destroys the heap-allocated shared_ptr (decrementing refcount).
    /// When refcount reaches 0, the EmitContext is freed.
    /// Called by Rust when a BridgeMessage is processed or dropped.
    void emit_context_drop(void* shared_ptr_handle);

    /// Get the raw EmitContext* from a shared_ptr handle.
    /// Used by the bridge thread to pass the raw pointer to bridge_emit.
    void* emit_context_get(void* shared_ptr_handle);
}
