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

#include <Catalogs/Source/PhysicalSourceTypes/BenchmarkSourceType.hpp>

namespace NES {

namespace detail {

struct MemoryAreaDeleter {
    void operator()(uint8_t* ptr) const { free(ptr); }
};

}// namespace detail

BenchmarkSourceType::BenchmarkSourceType(uint8_t* memoryArea,
                                         size_t memoryAreaSize,
                                         uint64_t numBuffersToProduce,
                                         uint64_t gatheringValue,
                                         GatheringMode::Value gatheringMode,
                                         SourceMode::Value sourceMode,
                                         uint64_t sourceAffinity)
    : PhysicalSourceType(BENCHMARK_SOURCE), memoryArea(memoryArea, detail::MemoryAreaDeleter()), memoryAreaSize(memoryAreaSize),
      numberOfBuffersToProduce(numBuffersToProduce), gatheringValue(gatheringValue), gatheringMode(gatheringMode),
      sourceMode(sourceMode), sourceAffinity(sourceAffinity) {}

BenchmarkSourceTypePtr BenchmarkSourceType::create(uint8_t* memoryArea,
                                                   size_t memoryAreaSize,
                                                   uint64_t numBuffersToProduce,
                                                   uint64_t gatheringValue,
                                                   const std::string& gatheringMode,
                                                   const std::string& sourceMode,
                                                   uint64_t sourceAffinity) {
    NES_ASSERT(memoryArea, "invalid memory area");
    auto gatheringModeEnum = GatheringMode::getFromString(gatheringMode);
    auto sourceModeEnum = SourceMode::getFromString(sourceMode);
    return std::make_shared<BenchmarkSourceType>(BenchmarkSourceType(memoryArea,
                                                                     memoryAreaSize,
                                                                     numBuffersToProduce,
                                                                     gatheringValue,
                                                                     gatheringModeEnum,
                                                                     sourceModeEnum,
                                                                     sourceAffinity));
}

const std::shared_ptr<uint8_t>& BenchmarkSourceType::getMemoryArea() const { return memoryArea; }

size_t BenchmarkSourceType::getMemoryAreaSize() const { return memoryAreaSize; }

uint64_t BenchmarkSourceType::getGatheringValue() const { return gatheringValue; }

GatheringMode::Value BenchmarkSourceType::getGatheringMode() const { return gatheringMode; }

SourceMode::Value BenchmarkSourceType::getSourceMode() const { return sourceMode; }

uint64_t BenchmarkSourceType::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

uint64_t BenchmarkSourceType::getSourceAffinity() const { return sourceAffinity; }

std::string BenchmarkSourceType::toString() {
    std::stringstream ss;
    ss << "LambdaSourceType => {\n";
    ss << "MemoryArea :" << memoryArea;
    ss << "MemoryAreaSize :" << memoryAreaSize;
    ss << "NumberOfBuffersToProduce :" << numberOfBuffersToProduce;
    ss << "GatheringValue :" << gatheringValue;
    ss << "GatheringMode :" << gatheringMode;
    ss << "SourceMode :" << sourceMode;
    ss << "SourceAffinity :" << sourceAffinity;
    ss << "\n}";
    return ss.str();
}

bool BenchmarkSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<BenchmarkSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<BenchmarkSourceType>();
    return memoryArea == otherSourceConfig->memoryArea && memoryAreaSize == otherSourceConfig->memoryAreaSize
        && numberOfBuffersToProduce == otherSourceConfig->numberOfBuffersToProduce
        && gatheringValue == otherSourceConfig->gatheringValue && gatheringMode == otherSourceConfig->gatheringMode
        && sourceMode == otherSourceConfig->sourceMode && sourceAffinity == otherSourceConfig->sourceAffinity;
}

}// namespace NES
