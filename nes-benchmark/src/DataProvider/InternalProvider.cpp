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

#include <DataProvider/InternalProvider.hpp>

namespace NES::Benchmark::DataProvision {
InternalProvider::InternalProvider(uint64_t id,
                                   DataProvider::DataProviderMode providerMode,
                                   std::vector<Runtime::TupleBuffer> preAllocatedBuffers)
    : DataProvider(id, providerMode), preAllocatedBuffers(preAllocatedBuffers) {}

std::vector<Runtime::TupleBuffer>& InternalProvider::getPreAllocatedBuffers() { return preAllocatedBuffers; }

std::optional<Runtime::TupleBuffer> InternalProvider::readNextBuffer(uint64_t sourceId) {
    // For now, we only have a single source
    ((void) sourceId);
    while (!started) {
        //wait with data production until the source is really started and also block if the source gets stopped
        usleep(std::chrono::microseconds(150).count());
    }

    if (!preAllocatedBuffers.empty()) {
        auto buffer = preAllocatedBuffers[currentlyEmittedBuffer % preAllocatedBuffers.size()];
        ++currentlyEmittedBuffer;

//        auto wrapBuffer = Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), this);
        auto currentTime = std::chrono::high_resolution_clock::now().time_since_epoch();
        auto timeStamp = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime).count();

//        wrapBuffer.setCreationTimestampInMS(timeStamp);
//        wrapBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
        return buffer;
    }

    return std::nullopt;
}

void InternalProvider::recyclePooledBuffer(Runtime::detail::MemorySegment*) {}
void InternalProvider::recycleUnpooledBuffer(Runtime::detail::MemorySegment*) {}

void InternalProvider::start() { started = true; }
void InternalProvider::stop() {
    started = false;
    preAllocatedBuffers.clear();
}
InternalProvider::~InternalProvider() {
    started = false;
    preAllocatedBuffers.clear();
}

}// namespace NES::Benchmark::DataProvision
