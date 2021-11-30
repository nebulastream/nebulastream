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

#include <Operators/LogicalOperators/Sources/MaterializedViewSourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <utility>

namespace NES::Experimental {

MaterializedViewSourceDescriptor::MaterializedViewSourceDescriptor(SchemaPtr schema,
                                                                   std::shared_ptr<uint8_t> memArea,
                                                                   size_t memAreaSize,
                                                                   uint64_t numBuffersToProcess,
                                                                   uint64_t mViewId)
    : SourceDescriptor(std::move(schema)), memArea(std::move(memArea)), memAreaSize(memAreaSize),
      numBuffersToProcess(numBuffersToProcess), mViewId(mViewId) {
    NES_ASSERT(this->memArea != nullptr && this->memAreaSize > 0, "invalid memory area");
}

std::shared_ptr<MaterializedViewSourceDescriptor>
MaterializedViewSourceDescriptor::create(const SchemaPtr& schema,
                                         const std::shared_ptr<uint8_t>& memArea,
                                         size_t memAreaSize,
                                         uint64_t numBuffersToProcess,
                                         uint64_t mViewId) {
    NES_ASSERT(memArea != nullptr && memAreaSize > 0, "invalid memory area");
    NES_ASSERT(schema, "invalid schema");
    return std::make_shared<MaterializedViewSourceDescriptor>(schema, memArea, memAreaSize, numBuffersToProcess, mViewId);
}
std::string MaterializedViewSourceDescriptor::toString() { return "MaterializedViewSourceDescriptor"; }

bool MaterializedViewSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<MaterializedViewSourceDescriptor>()) {
        return false;
    }
    auto otherMemDescr = other->as<MaterializedViewSourceDescriptor>();
    return schema == otherMemDescr->schema;
}

std::shared_ptr<uint8_t> MaterializedViewSourceDescriptor::getMemArea() { return memArea; }
size_t MaterializedViewSourceDescriptor::getMemAreaSize() const { return memAreaSize; }
uint64_t MaterializedViewSourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }
uint64_t MaterializedViewSourceDescriptor::getMViewId() const { return mViewId; }
}// namespace NES::Experimental