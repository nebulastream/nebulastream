/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
#include <utility>

namespace NES {

MemorySourceDescriptor::MemorySourceDescriptor(SchemaPtr schema, std::shared_ptr<uint8_t> memoryArea, size_t memoryAreaSize, uint64_t numBuffersToProcess, uint64_t frequency)
    : SourceDescriptor(std::move(schema)), memoryArea(memoryArea), memoryAreaSize(memoryAreaSize),
      numBuffersToProcess(numBuffersToProcess), frequency(frequency) {
    NES_ASSERT(this->memoryArea != nullptr && this->memoryAreaSize > 0, "invalid memory area");
}

std::shared_ptr<MemorySourceDescriptor> MemorySourceDescriptor::create(SchemaPtr schema, std::shared_ptr<uint8_t> memoryArea,
                                                                       size_t memoryAreaSize, uint64_t numBuffersToProcess, uint64_t frequency) {
    NES_ASSERT(memoryArea != nullptr && memoryAreaSize > 0, "invalid memory area");
    NES_ASSERT(schema, "invalid schema");
    return std::make_shared<MemorySourceDescriptor>(schema, memoryArea, memoryAreaSize, numBuffersToProcess, frequency);
}
std::string MemorySourceDescriptor::toString() { return "MemorySourceDescriptor"; }

bool MemorySourceDescriptor::equal(SourceDescriptorPtr other) {
    if (!other->instanceOf<MemorySourceDescriptor>()) {
        return false;
    }
    auto otherMemDescr = other->as<MemorySourceDescriptor>();
    return schema == otherMemDescr->schema;
}

std::shared_ptr<uint8_t> MemorySourceDescriptor::getMemoryArea() { return memoryArea; }

size_t MemorySourceDescriptor::getMemoryAreaSize() const { return memoryAreaSize; }
uint64_t MemorySourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }
uint64_t MemorySourceDescriptor::getFrequency() const { return frequency; }
}// namespace NES