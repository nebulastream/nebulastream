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
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalSinkOperator::PhysicalSinkOperator(OperatorId id,
                                           SchemaPtr inputSchema,
                                           SchemaPtr outputSchema,
                                           SinkDescriptorPtr sinkDescriptor,
                                           uint16_t numberOfInputSources)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      sinkDescriptor(std::move(sinkDescriptor)), numberOfInputSources(numberOfInputSources) {}

PhysicalOperatorPtr PhysicalSinkOperator::create(SchemaPtr inputSchema,
                                                 SchemaPtr outputSchema,
                                                 SinkDescriptorPtr sinkDescriptor,
                                                 uint16_t numberOfInputSources) {
    return create(getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(sinkDescriptor),
                  numberOfInputSources);
}

PhysicalOperatorPtr PhysicalSinkOperator::create(OperatorId id,
                                                 const SchemaPtr& inputSchema,
                                                 const SchemaPtr& outputSchema,
                                                 const SinkDescriptorPtr& sinkDescriptor,
                                                 uint16_t numberOfInputSources) {
    return std::make_shared<PhysicalSinkOperator>(id, inputSchema, outputSchema, sinkDescriptor, numberOfInputSources);
}

SinkDescriptorPtr PhysicalSinkOperator::getSinkDescriptor() { return sinkDescriptor; }

std::string PhysicalSinkOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalSinkOperator:\n";
    out << PhysicalUnaryOperator::toString();
    if (sinkDescriptor != nullptr) {
        out << sinkDescriptor->toString();
    }
    out << std::endl;
    return out.str();
}

OperatorNodePtr PhysicalSinkOperator::copy() {
    auto result = create(id, inputSchema, outputSchema, sinkDescriptor, numberOfInputSources);
    result->addAllProperties(properties);
    return result;
}

uint16_t PhysicalSinkOperator::getNumberOfInputSources() const { return numberOfInputSources; }
}// namespace NES::QueryCompilation::PhysicalOperators
