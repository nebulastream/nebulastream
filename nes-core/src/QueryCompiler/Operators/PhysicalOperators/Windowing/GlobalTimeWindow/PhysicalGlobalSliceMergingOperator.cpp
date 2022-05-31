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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <memory>
namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalGlobalSliceMergingOperator::PhysicalGlobalSliceMergingOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalSliceMergingOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractScanOperator(),
      operatorHandler(operatorHandler) {}
std::string PhysicalGlobalSliceMergingOperator::toString() const { return "PhysicalGlobalSliceMergingOperator"; }

std::shared_ptr<PhysicalGlobalSliceMergingOperator> PhysicalGlobalSliceMergingOperator::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalSliceMergingOperatorHandlerPtr keyedEventTimeWindowHandler) {
    return std::make_shared<PhysicalGlobalSliceMergingOperator>(Util::getNextOperatorId(),
                                                                inputSchema,
                                                                outputSchema,
                                                                keyedEventTimeWindowHandler);
}

Windowing::Experimental::GlobalSliceMergingOperatorHandlerPtr PhysicalGlobalSliceMergingOperator::getWindowHandler() {
    return operatorHandler;
}

OperatorNodePtr PhysicalGlobalSliceMergingOperator::copy() { return create(inputSchema, outputSchema, operatorHandler); }
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES