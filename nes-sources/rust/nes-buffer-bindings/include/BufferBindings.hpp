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
#include <BackpressureChannel.hpp>
#include <atomic>
#include <memory>
#include <cstdint>

namespace NES::BufferBindings {

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

// Child buffer access for variable-sized data
uint32_t getNumberOfChildBuffers(const NES::TupleBuffer& buf);
std::unique_ptr<NES::TupleBuffer> loadChildBuffer(const NES::TupleBuffer& buf, uint32_t index);

// AbstractBufferProvider operations
std::unique_ptr<NES::TupleBuffer> getBufferBlocking(NES::AbstractBufferProvider& provider);
std::unique_ptr<NES::TupleBuffer> tryGetBuffer(NES::AbstractBufferProvider& provider);
uint64_t getBufferSize(const NES::AbstractBufferProvider& provider);

// Buffer recycle notification -- installs the Rust async wakeup callback
void installBufferRecycleNotification();

// BackpressureListener operations
void waitForBackpressure(const BackpressureListener& listener);

} // namespace NES::BufferBindings

// CXX bridge generates code that references types and functions at global scope.
// These using declarations expose the NES types and BufferBindings functions
// so that the CXX-generated cpp files (which include this header) can resolve
// ::TupleBuffer, ::retain, ::getDataPtr etc. without namespace qualification.
using NES::TupleBuffer;
using NES::AbstractBufferProvider;
using namespace NES::BufferBindings;

// --- Error context struct (shared by source and sink bindings) ---

namespace NES {

/// Context passed to on_source_error_callback / on_sink_error_callback.
/// Holds the source_id/sink_id for logging context. Owned by TokioSource/TokioSink.
struct ErrorContext {
    uint64_t sourceId;
};

} // namespace NES

/// Releases a Rust semaphore slot. Called from C++ OnComplete callbacks
/// when the pipeline finishes processing a TokioSource buffer.
/// Defined in Rust with #[no_mangle] extern "C" linkage.
extern "C" void nes_release_semaphore_slot(const void* semaphore_ptr);
