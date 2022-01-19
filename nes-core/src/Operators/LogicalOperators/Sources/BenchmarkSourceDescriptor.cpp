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

#include <Operators/LogicalOperators/Sources/BenchmarkSourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <utility>

namespace NES {

BenchmarkSourceDescriptor::BenchmarkSourceDescriptor(SchemaPtr schema,
                                                     std::shared_ptr<uint8_t> memoryArea,
                                                     size_t memoryAreaSize,
                                                     uint64_t numBuffersToProcess,
                                                     uint64_t gatheringValue,
                                                     GatheringMode::Value gatheringMode,
                                                     BenchmarkSource::SourceMode sourceMode,
                                                     uint64_t sourceAffinity)
    : SourceDescriptor(std::move(schema)), memoryArea(std::move(memoryArea)), memoryAreaSize(memoryAreaSize),
      numBuffersToProcess(numBuffersToProcess), gatheringValue(gatheringValue), gatheringMode(gatheringMode),
      sourceMode(sourceMode), sourceAffinity(sourceAffinity) {
    NES_ASSERT(this->memoryArea != nullptr && this->memoryAreaSize > 0, "invalid memory area");
}

std::shared_ptr<BenchmarkSourceDescriptor> BenchmarkSourceDescriptor::create(const SchemaPtr& schema,
                                                                             const std::shared_ptr<uint8_t>& memoryArea,
                                                                             size_t memoryAreaSize,
                                                                             uint64_t numBuffersToProcess,
                                                                             uint64_t gatheringValue,
                                                                             GatheringMode::Value gatheringMode,
                                                                             BenchmarkSource::SourceMode sourceMode,
                                                                             uint64_t sourceAffinity) {
    NES_ASSERT(memoryArea != nullptr && memoryAreaSize > 0, "invalid memory area");
    NES_ASSERT(schema, "invalid schema");
    return std::make_shared<BenchmarkSourceDescriptor>(schema,
                                                       memoryArea,
                                                       memoryAreaSize,
                                                       numBuffersToProcess,
                                                       gatheringValue,
                                                       gatheringMode,
                                                       sourceMode,
                                                       sourceAffinity);
}
std::string BenchmarkSourceDescriptor::toString() { return "BenchmarkSourceDescriptor"; }

bool BenchmarkSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<BenchmarkSourceDescriptor>()) {
        return false;
    }
    auto otherMemDescr = other->as<BenchmarkSourceDescriptor>();
    return schema == otherMemDescr->schema;
}

std::shared_ptr<uint8_t> BenchmarkSourceDescriptor::getMemoryArea() { return memoryArea; }

size_t BenchmarkSourceDescriptor::getMemoryAreaSize() const { return memoryAreaSize; }
uint64_t BenchmarkSourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }
uint64_t BenchmarkSourceDescriptor::getSourceAffinity() const { return sourceAffinity; }
DataSource::GatheringMode BenchmarkSourceDescriptor::getGatheringMode() const { return gatheringMode; }
BenchmarkSource::SourceMode BenchmarkSourceDescriptor::getSourceMode() const { return sourceMode; }
uint64_t BenchmarkSourceDescriptor::getGatheringValue() const { return gatheringValue; }
}// namespace NES