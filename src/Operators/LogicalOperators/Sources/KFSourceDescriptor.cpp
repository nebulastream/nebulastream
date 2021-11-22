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

#include <Catalogs/KFSourceStreamConfig.hpp>
#include <Operators/LogicalOperators/Sources/KFSourceDescriptor.hpp>

namespace NES {

KFSourceDescriptor::KFSourceDescriptor(SchemaPtr schema,
                                       std::shared_ptr<uint8_t> memoryArea,
                                       size_t memoryAreaSize,
                                       uint64_t numBuffersToProcess,
                                       uint64_t gatheringValue,
                                       uint64_t sourceAffinity)
    : SourceDescriptor(std::move(schema)),
      memoryArea(std::move(memoryArea)),
      memoryAreaSize(std::move(memoryAreaSize)),
      numBuffersToProcess(numBuffersToProcess),
      gatheringValue(gatheringValue),
      sourceAffinity(sourceAffinity) {
    this->gatheringMode = DataSource::GatheringMode::FREQUENCY_MODE;
    NES_ASSERT(this->memoryArea != nullptr && this->memoryAreaSize > 0, "invalid memory area");
}

std::shared_ptr<KFSourceDescriptor> KFSourceDescriptor::create(const SchemaPtr& schema,
                                                               const std::shared_ptr<uint8_t>& memoryArea,
                                                               size_t memoryAreaSize,
                                                               uint64_t numBuffersToProcess,
                                                               uint64_t gatheringValue,
                                                               uint64_t sourceAffinity) {
    NES_ASSERT(memoryArea != nullptr && memoryAreaSize > 0, "invalid memory area");
    NES_ASSERT(schema, "invalid schema");
    return std::make_shared<KFSourceDescriptor>(schema, memoryArea, memoryAreaSize,
                                                numBuffersToProcess, gatheringValue,
                                                sourceAffinity);
}

std::string KFSourceDescriptor::toString() { return "KFSourceDescriptor"; }

bool KFSourceDescriptor::equal(const SourceDescriptorPtr& other) {
    if (other->instanceOf<KFSourceDescriptor>()) {
        return false;
    }
    auto otherDescr = other->as<KFSourceDescriptor>();
    return schema == otherDescr->getSchema();
}

std::shared_ptr<uint8_t> KFSourceDescriptor::getMemoryArea() { return memoryArea; }
size_t KFSourceDescriptor::getMemoryAreaSize() const { return memoryAreaSize; }
uint64_t KFSourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }
uint64_t KFSourceDescriptor::getSourceAffinity() const { return sourceAffinity; }
DataSource::GatheringMode KFSourceDescriptor::getGatheringMode() const { return gatheringMode; }
uint64_t KFSourceDescriptor::getGatheringValue() const { return gatheringValue; }

}// namespace NES
