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

#include <BufferBindings.hpp>

#include <cstdint>
#include <memory>
#include <stop_token>

#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>

#include <nes-buffer-bindings/lib.h>

namespace NES::BufferBindings {

void retain(NES::TupleBuffer& buf)
{
    buf.retain();
}

void release(NES::TupleBuffer& buf)
{
    buf.release();
}

const uint8_t* getDataPtr(const NES::TupleBuffer& buf)
{
    return buf.getAvailableMemoryArea<uint8_t>().data();
}

uint8_t* getDataPtrMut(NES::TupleBuffer& buf)
{
    return buf.getAvailableMemoryArea<uint8_t>().data();
}

uint64_t getCapacity(const NES::TupleBuffer& buf)
{
    return buf.getBufferSize();
}

uint64_t getNumberOfTuples(const NES::TupleBuffer& buf)
{
    return buf.getNumberOfTuples();
}

void setNumberOfTuples(NES::TupleBuffer& buf, uint64_t count)
{
    buf.setNumberOfTuples(count);
}

uint32_t getReferenceCounter(const NES::TupleBuffer& buf)
{
    return buf.getReferenceCounter();
}

std::unique_ptr<NES::TupleBuffer> cloneTupleBuffer(const NES::TupleBuffer& buf)
{
    return std::make_unique<NES::TupleBuffer>(buf);
}

uint32_t getNumberOfChildBuffers(const NES::TupleBuffer& buf)
{
    return buf.getNumberOfChildBuffers();
}

std::unique_ptr<NES::TupleBuffer> loadChildBuffer(const NES::TupleBuffer& buf, uint32_t index)
{
    auto child = buf.loadChildBuffer(NES::VariableSizedAccess::Index(index));
    return std::make_unique<NES::TupleBuffer>(std::move(child));
}

std::unique_ptr<NES::TupleBuffer> getBufferBlocking(NES::AbstractBufferProvider& provider)
{
    auto tb = provider.getBufferBlocking();
    return std::make_unique<NES::TupleBuffer>(std::move(tb));
}

std::unique_ptr<NES::TupleBuffer> tryGetBuffer(NES::AbstractBufferProvider& provider)
{
    auto opt = provider.getBufferNoBlocking();
    if (opt.has_value())
    {
        return std::make_unique<NES::TupleBuffer>(std::move(opt.value()));
    }
    return nullptr;
}

uint64_t getBufferSize(const NES::AbstractBufferProvider& provider)
{
    return static_cast<uint64_t>(provider.getBufferSize());
}

// Defined in Rust with #[no_mangle] extern "C" linkage
extern "C" void nes_on_buffer_recycled();

void installBufferRecycleNotification()
{
    NES::setBufferRecycleNotification(nes_on_buffer_recycled);
}

void waitForBackpressure(const BackpressureListener& listener)
{
    listener.wait(std::stop_token{});
}

} // namespace NES::BufferBindings
