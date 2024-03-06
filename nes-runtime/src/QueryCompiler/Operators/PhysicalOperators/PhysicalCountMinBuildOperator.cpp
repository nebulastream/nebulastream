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

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalCountMinBuildOperator.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptor.hpp>
#include <Execution/Operators/Statistics/CountMinOperatorHandler.hpp>
#include <sstream>

namespace NES::Experimental::Statistics {

PhysicalCountMinBuildOperator::PhysicalCountMinBuildOperator(OperatorId operatorId,
                                                             WindowStatisticDescriptorPtr windowStatisticDescriptor,
                                                             SchemaPtr inputSchema,
                                                             SchemaPtr outputSchema,
                                                             const CountMinOperatorHandlerPtr& operatorHandler)
    : OperatorNode(operatorId), PhysicalUnaryOperator(operatorId, std::move(inputSchema), std::move(outputSchema)),
      descriptor(std::move(windowStatisticDescriptor)), countMinOperatorHandler(std::move(operatorHandler)) {}

QueryCompilation::PhysicalOperators::PhysicalOperatorPtr
PhysicalCountMinBuildOperator::create(const OperatorId operatorId,
                                      const WindowStatisticDescriptorPtr& windowStatisticDescriptor,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const CountMinOperatorHandlerPtr& operatorHandler) {
    return std::make_shared<PhysicalCountMinBuildOperator>(operatorId, windowStatisticDescriptor, inputSchema, outputSchema, operatorHandler);
}

QueryCompilation::PhysicalOperators::PhysicalOperatorPtr
PhysicalCountMinBuildOperator::create(const WindowStatisticDescriptorPtr& windowStatisticDescriptor,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const CountMinOperatorHandlerPtr& operatorHandler) {
    return create(getNextOperatorId(), windowStatisticDescriptor, inputSchema, outputSchema, operatorHandler);
}

std::string PhysicalCountMinBuildOperator::toString() const {
    std::stringstream ss;
    ss << std::endl;
    ss << "PhysicalCountMinBuildOperator:\n";
    ss << getDescriptor()->toString();
    ss << PhysicalUnaryOperator::toString();
    return ss.str();
}

OperatorNodePtr PhysicalCountMinBuildOperator::copy() {
    auto result = create(id, getDescriptor(), inputSchema, outputSchema, countMinOperatorHandler);
    result->addAllProperties(properties);
    return result;
}

const WindowStatisticDescriptorPtr& PhysicalCountMinBuildOperator::getDescriptor() const { return descriptor; }

const CountMinOperatorHandlerPtr& PhysicalCountMinBuildOperator::getCountMinOperatorHandler() const {
    return countMinOperatorHandler;
}

}// namespace NES::Experimental::Statistics