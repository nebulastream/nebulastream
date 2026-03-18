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

#include <Runtime/TupleBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <BackpressureChannel.hpp>
#include <atomic>
#include <memory>
#include <cstdint>
#include <functional>
#include <stop_token>

namespace NES::IoBindings {

// TupleBuffer operations
void retain(NES::TupleBuffer& buf);
void release(NES::TupleBuffer& buf);
const uint8_t* getDataPtr(const NES::TupleBuffer& buf);
uint8_t* getDataPtrMut(NES::TupleBuffer& buf);
uint64_t getCapacity(const NES::TupleBuffer& buf);
uint64_t getNumberOfTuples(const NES::TupleBuffer& buf);
void setNumberOfTuples(NES::TupleBuffer& buf, uint64_t count);
uint32_t getReferenceCounter(const NES::TupleBuffer& buf);
std::unique_ptr<NES::TupleBuffer> cloneTupleBuffer(const NES::TupleBuffer& buf);

// AbstractBufferProvider operations
std::unique_ptr<NES::TupleBuffer> getBufferBlocking(NES::AbstractBufferProvider& provider);
std::unique_ptr<NES::TupleBuffer> tryGetBuffer(NES::AbstractBufferProvider& provider);
uint64_t getBufferSize(const NES::AbstractBufferProvider& provider);

// Buffer recycle notification -- installs the Rust async wakeup callback
void installBufferRecycleNotification();

// BackpressureListener operations
void waitForBackpressure(const BackpressureListener& listener);

} // namespace NES::IoBindings

// CXX bridge generates code that references types and functions at global scope.
// These using declarations expose the NES types and IoBindings functions
// so that the CXX-generated cpp files (which include this header) can resolve
// ::TupleBuffer, ::retain, ::getDataPtr etc. without namespace qualification.
using NES::TupleBuffer;
using NES::AbstractBufferProvider;
using namespace NES::IoBindings;

// --- Phase 2: Emit and error callback contexts and C-linkage declarations ---
//
// These structs and functions form the C++ side of the Rust->C++ callback interface.
// The Rust bridge thread calls bridge_emit to dispatch buffers to the C++ task queue.
// The Rust monitoring task calls on_source_error_callback when a source errors/panics.

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

/// Context passed to on_source_error_callback as the opaque void* context pointer.
/// Holds the source_id for logging context. Owned by TokioSource.
struct ErrorContext {
    uint64_t sourceId;
};

} // namespace NES

/// Releases a Rust semaphore slot. Called from C++ OnComplete callbacks
/// when the pipeline finishes processing a TokioSource buffer.
/// Defined in Rust with #[no_mangle] extern "C" linkage.
extern "C" void nes_release_semaphore_slot(const void* semaphore_ptr);

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

    /// Called by Rust monitoring task when a sink errors or panics.
    void on_sink_error_callback(
        void* context,        // ErrorContext*
        uint64_t sink_id,
        const char* message
    );
}
