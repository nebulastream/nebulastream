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

#include <Configurations/Worker/PhysicalSourceTypes/BenchmarkSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {

namespace detail {

struct MemoryAreaDeleter {
    void operator()(uint8_t* ptr) const { free(ptr); }
};

}// namespace detail

BenchmarkSourceType::BenchmarkSourceType(std::string logicalSourceName, std::string physicalSourceName,uint8_t* memoryArea,
                                         size_t memoryAreaSize,
                                         uint64_t numBuffersToProduce,
                                         uint64_t gatheringValue,
                                         GatheringMode gatheringMode,
                                         SourceMode sourceMode,
                                         uint64_t sourceAffinity,
                                         uint64_t taskQueueId)
    : PhysicalSourceType(std::move(logicalSourceName), std::move(physicalSourceName),SourceType::BENCHMARK_SOURCE), memoryArea(memoryArea, detail::MemoryAreaDeleter()),
      memoryAreaSize(memoryAreaSize), numberOfBuffersToProduce(numBuffersToProduce), gatheringValue(gatheringValue),
      gatheringMode(gatheringMode), sourceMode(sourceMode), sourceAffinity(sourceAffinity), taskQueueId(taskQueueId) {}

BenchmarkSourceTypePtr BenchmarkSourceType::create(std::string logicalSourceName, std::string physicalSourceName,uint8_t* memoryArea,
                                                   size_t memoryAreaSize,
                                                   uint64_t numBuffersToProduce,
                                                   uint64_t gatheringValue,
                                                   GatheringMode gatheringMode,
                                                   SourceMode sourceMode,
                                                   uint64_t sourceAffinity,
                                                   uint64_t taskQueueId) {
    NES_ASSERT(memoryArea, "invalid memory area");
    return std::make_shared<BenchmarkSourceType>(BenchmarkSourceType(std::move(logicalSourceName), std::move(physicalSourceName),memoryArea,
                                                                     memoryAreaSize,
                                                                     numBuffersToProduce,
                                                                     gatheringValue,
                                                                     gatheringMode,
                                                                     sourceMode,
                                                                     sourceAffinity,
                                                                     taskQueueId));
}

const std::shared_ptr<uint8_t>& BenchmarkSourceType::getMemoryArea() const { return memoryArea; }

size_t BenchmarkSourceType::getMemoryAreaSize() const { return memoryAreaSize; }

uint64_t BenchmarkSourceType::getGatheringValue() const { return gatheringValue; }

GatheringMode BenchmarkSourceType::getGatheringMode() const { return gatheringMode; }

SourceMode BenchmarkSourceType::getSourceMode() const { return sourceMode; }

uint64_t BenchmarkSourceType::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

uint64_t BenchmarkSourceType::getSourceAffinity() const { return sourceAffinity; }
uint64_t BenchmarkSourceType::getTaskQueueId() const { return taskQueueId; }

std::string BenchmarkSourceType::toString() {
    std::stringstream ss;
    ss << "LambdaSourceType => {\n";
    ss << "MemoryArea :" << memoryArea;
    ss << "MemoryAreaSize :" << memoryAreaSize;
    ss << "NumberOfBuffersToProduce :" << numberOfBuffersToProduce;
    ss << "GatheringValue :" << gatheringValue;
    ss << "GatheringMode :" << magic_enum::enum_name(gatheringMode);
    ss << "SourceMode :" << magic_enum::enum_name(sourceMode);
    ss << "SourceAffinity :" << sourceAffinity;
    ss << "taskQueueId :" << taskQueueId;
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

void BenchmarkSourceType::reset() {
    //nothing
}

}// namespace NES
