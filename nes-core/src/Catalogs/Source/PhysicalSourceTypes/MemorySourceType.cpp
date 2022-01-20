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

#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>

namespace NES {

using namespace Configurations;

namespace detail {

struct MemoryAreaDeleter {
    void operator()(uint8_t* ptr) const { free(ptr); }
};

}// namespace detail

MemorySourceType::MemorySourceType(uint8_t* memoryArea,
                                   size_t memoryAreaSize,
                                   uint64_t numBuffersToProduce,
                                   uint64_t gatheringValue,
                                   GatheringMode::Value gatheringMode)
    : PhysicalSourceType(MEMORY_SOURCE), memoryArea(memoryArea, detail::MemoryAreaDeleter()), memoryAreaSize(memoryAreaSize),
      numberOfBufferToProduce(numBuffersToProduce), gatheringValue(gatheringValue), gatheringMode(gatheringMode) {}

MemorySourceTypePtr MemorySourceType::create(uint8_t* memoryArea,
                                             size_t memoryAreaSize,
                                             uint64_t numBuffersToProcess,
                                             uint64_t gatheringValue,
                                             const std::string& gatheringMode) {
    NES_ASSERT(memoryArea, "invalid memory area");
    auto gatheringModeEnum = GatheringMode::getFromString(gatheringMode);
    return std::make_shared<MemorySourceType>(
        MemorySourceType(memoryArea, memoryAreaSize, numBuffersToProcess, gatheringValue, gatheringModeEnum));
}

const std::shared_ptr<uint8_t>& MemorySourceType::getMemoryArea() const { return memoryArea; }

size_t MemorySourceType::getMemoryAreaSize() const { return memoryAreaSize; }

uint64_t MemorySourceType::getNumberOfBufferToProduce() const { return numberOfBufferToProduce; }

uint64_t MemorySourceType::getGatheringValue() const { return gatheringValue; }

GatheringMode::Value MemorySourceType::getGatheringMode() const { return gatheringMode; }

std::string MemorySourceType::toString() {
    std::stringstream ss;
    ss << "LambdaSourceType => {\n";
    ss << "MemoryArea :" << memoryArea;
    ss << "MemoryAreaSize :" << memoryAreaSize;
    ss << "NumberOfBuffersToProduce :" << numberOfBufferToProduce;
    ss << "GatheringValue :" << gatheringValue;
    ss << "GatheringMode :" << GatheringMode::toString(gatheringMode);
    ss << "\n}";
    return ss.str();
}

bool MemorySourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<MemorySourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<MemorySourceType>();
    return memoryArea == otherSourceConfig->memoryArea && memoryAreaSize == otherSourceConfig->memoryAreaSize
        && numberOfBufferToProduce == otherSourceConfig->numberOfBufferToProduce
        && gatheringValue == otherSourceConfig->gatheringValue && gatheringMode == otherSourceConfig->gatheringMode;
}

void MemorySourceType::reset() {
    //Nothing
}

}// namespace NES
