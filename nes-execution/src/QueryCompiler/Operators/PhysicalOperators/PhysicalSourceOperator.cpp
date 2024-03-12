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
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalSourceOperator::PhysicalSourceOperator(OperatorId id,
                                               StatisticId statisticId,
                                               OriginId originId,
                                               SchemaPtr inputSchema,
                                               SchemaPtr outputSchema,
                                               SourceDescriptorPtr sourceDescriptor)
    : Operator(id), PhysicalUnaryOperator(id, statisticId, std::move(inputSchema), std::move(outputSchema)),
      sourceDescriptor(std::move(sourceDescriptor)), originId(originId) {}

std::shared_ptr<PhysicalSourceOperator> PhysicalSourceOperator::create(OperatorId id,
                                                                       StatisticId statisticId,
                                                                       OriginId originId,
                                                                       const SchemaPtr& inputSchema,
                                                                       const SchemaPtr& outputSchema,
                                                                       const SourceDescriptorPtr& sourceDescriptor) {
    return std::make_shared<PhysicalSourceOperator>(id, statisticId, originId, inputSchema, outputSchema, sourceDescriptor);
}

std::shared_ptr<PhysicalSourceOperator>
PhysicalSourceOperator::create(StatisticId statisticId, SchemaPtr inputSchema, SchemaPtr outputSchema, SourceDescriptorPtr sourceDescriptor) {
    return create(getNextOperatorId(), statisticId, INVALID_ORIGIN_ID, std::move(inputSchema), std::move(outputSchema), std::move(sourceDescriptor));
}

OriginId PhysicalSourceOperator::getOriginId() { return originId; }

void PhysicalSourceOperator::setOriginId(OriginId originId) { this->originId = originId; }

SourceDescriptorPtr PhysicalSourceOperator::getSourceDescriptor() { return sourceDescriptor; }

std::string PhysicalSourceOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalSourceOperator:\n";
    out << PhysicalUnaryOperator::toString();
    if (sourceDescriptor != nullptr) {
        out << sourceDescriptor->toString() << "\n";
    }
    out << "originId: " << originId;
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalSourceOperator::copy() {
    auto result = create(id, statisticId, originId, inputSchema, outputSchema, sourceDescriptor);
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators