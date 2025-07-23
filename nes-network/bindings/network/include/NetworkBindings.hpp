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

#include <cstdint>
#include <rust/cxx.h>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

struct SerializedTupleBufferHeader;

/// The TupleBufferBuilder is a wrapper around a tuple buffer with methods exposed to rust.
/// This allows the rust code to set metadata on a tuple buffer without directly exposing the TupleBuffer and its control block
/// to rust.
/// It is important that the underlying TupleBuffer outlives the TupleBufferBuilder
class TupleBufferBuilder
{
public:
    explicit TupleBufferBuilder(NES::TupleBuffer& buffer, NES::AbstractBufferProvider& bufferProvider)
        : buffer(buffer), bufferProvider(bufferProvider)
    {
    }

    void setMetadata(const SerializedTupleBufferHeader&);
    void setData(rust::Slice<const uint8_t>);
    void addChildBuffer(rust::Slice<const uint8_t>);

private:
    /// The wrapper is only a temporary object and thus does not store anything by value.
    NES::TupleBuffer& buffer; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    NES::AbstractBufferProvider& bufferProvider; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

/// This function is required to allow threads from rust to set thread local variables required for logger context.
/// I.e., threads created by the tokio runtime need to be associated with a worker and a thread name
void identifyThread(rust::str threadName, rust::str workerId);
