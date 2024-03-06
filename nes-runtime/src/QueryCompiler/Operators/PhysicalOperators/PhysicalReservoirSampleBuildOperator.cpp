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

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalReservoirSampleBuildOperator.hpp>
#include <sstream>

namespace NES::Experimental::Statistics {

PhysicalReservoirSampleBuildOperator::PhysicalReservoirSampleBuildOperator(OperatorId operatorId,
                                                                           WindowStatisticDescriptorPtr windowStatisticDescriptor,
                                                                           SchemaPtr inputSchema,
                                                                           SchemaPtr outputSchema)
    : OperatorNode(operatorId), PhysicalUnaryOperator(operatorId, std::move(inputSchema), std::move(outputSchema)),
      descriptor(std::move(windowStatisticDescriptor)) {}

QueryCompilation::PhysicalOperators::PhysicalOperatorPtr
PhysicalReservoirSampleBuildOperator::create(const OperatorId operatorId,
                                             const WindowStatisticDescriptorPtr& windowStatisticDescriptor,
                                             const SchemaPtr& inputSchema,
                                             const SchemaPtr& outputSchema) {
    return std::make_shared<PhysicalReservoirSampleBuildOperator>(operatorId,
                                                                  windowStatisticDescriptor,
                                                                  inputSchema,
                                                                  outputSchema);
}
QueryCompilation::PhysicalOperators::PhysicalOperatorPtr
PhysicalReservoirSampleBuildOperator::create(const WindowStatisticDescriptorPtr& windowStatisticDescriptor,
                                             const SchemaPtr& inputSchema,
                                             const SchemaPtr& outputSchema) {
    return create(getNextOperatorId(), windowStatisticDescriptor, inputSchema, outputSchema);
}

std::string PhysicalReservoirSampleBuildOperator::toString() const {
    std::stringstream ss;
    ss << std::endl;
    ss << "PhysicalReservoirSampleOperator:\n";
    ss << getDescriptor()->toString();
    ss << PhysicalUnaryOperator::toString();
    return ss.str();
}

OperatorNodePtr PhysicalReservoirSampleBuildOperator::copy() {
    auto result = create(id, getDescriptor(), inputSchema, outputSchema);
    result->addAllProperties(properties);
    return result;
}

const WindowStatisticDescriptorPtr& PhysicalReservoirSampleBuildOperator::getDescriptor() const { return descriptor; }
}// namespace NES::Experimental::Statistics