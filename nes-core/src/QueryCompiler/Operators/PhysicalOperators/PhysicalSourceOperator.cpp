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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalSourceOperator::PhysicalSourceOperator(OperatorId id,
                                               OriginId originId,
                                               SchemaPtr inputSchema,
                                               SchemaPtr outputSchema,
                                               SourceDescriptorPtr sourceDescriptor)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      sourceDescriptor(std::move(sourceDescriptor)), originId(originId) {}

std::shared_ptr<PhysicalSourceOperator> PhysicalSourceOperator::create(OperatorId id,
                                                                       OriginId originId,
                                                                       const SchemaPtr& inputSchema,
                                                                       const SchemaPtr& outputSchema,
                                                                       const SourceDescriptorPtr& sourceDescriptor) {
    return std::make_shared<PhysicalSourceOperator>(id, originId, inputSchema, outputSchema, sourceDescriptor);
}

std::shared_ptr<PhysicalSourceOperator>
PhysicalSourceOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, SourceDescriptorPtr sourceDescriptor) {
    return create(Util::getNextOperatorId(), 0, std::move(inputSchema), std::move(outputSchema), std::move(sourceDescriptor));
}

uint64_t PhysicalSourceOperator::getOriginId() { return originId; }

void PhysicalSourceOperator::setOriginId(OriginId originId) { this->originId = originId; }

SourceDescriptorPtr PhysicalSourceOperator::getSourceDescriptor() { return sourceDescriptor; }

std::string PhysicalSourceOperator::toString() const { return "PhysicalSourceOperator"; }

OperatorNodePtr PhysicalSourceOperator::copy() {
    auto result = create(id, originId, inputSchema, outputSchema, sourceDescriptor);
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators