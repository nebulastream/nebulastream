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

#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <utility>

namespace NES {

LambdaSourceDescriptor::LambdaSourceDescriptor(
    SchemaPtr schema,
    std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
    uint64_t numBuffersToProduce,
    uint64_t gatheringValue,
    GatheringMode::Value gatheringMode)
    : SourceDescriptor(std::move(schema)), generationFunction(std::move(generationFunction)),
      numBuffersToProcess(numBuffersToProduce), gatheringValue(gatheringValue), gatheringMode(gatheringMode) {}

std::shared_ptr<LambdaSourceDescriptor> LambdaSourceDescriptor::create(
    const SchemaPtr& schema,
    std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
    uint64_t numBuffersToProcess,
    uint64_t gatheringValue,
    GatheringMode::Value gatheringMode) {
    NES_ASSERT(schema, "invalid schema");
    return std::make_shared<LambdaSourceDescriptor>(schema,
                                                    std::move(generationFunction),
                                                    numBuffersToProcess,
                                                    gatheringValue,
                                                    gatheringMode);
}
std::string LambdaSourceDescriptor::toString() { return "LambdaSourceDescriptor"; }

bool LambdaSourceDescriptor::equal(SourceDescriptorPtr const& other) {
    if (!other->instanceOf<LambdaSourceDescriptor>()) {
        return false;
    }
    auto otherMemDescr = other->as<LambdaSourceDescriptor>();
    return schema == otherMemDescr->schema;
}

std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&&
LambdaSourceDescriptor::getGeneratorFunction() {
    return std::move(generationFunction);
}

uint64_t LambdaSourceDescriptor::getNumBuffersToProcess() const { return numBuffersToProcess; }

GatheringMode::Value LambdaSourceDescriptor::getGatheringMode() const { return gatheringMode; }

uint64_t LambdaSourceDescriptor::getGatheringValue() const { return gatheringValue; }

SourceDescriptorPtr LambdaSourceDescriptor::copy() {
    auto copy = LambdaSourceDescriptor::create(schema->copy(),
                                               std::move(generationFunction),
                                               numBuffersToProcess,
                                               gatheringValue,
                                               gatheringMode);
    copy->setPhysicalSourceName(physicalSourceName);
    return copy;
}

}// namespace NES