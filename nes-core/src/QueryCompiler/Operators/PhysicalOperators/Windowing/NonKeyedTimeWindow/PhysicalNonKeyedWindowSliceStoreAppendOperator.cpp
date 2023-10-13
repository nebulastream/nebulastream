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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedWindowSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <memory>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalNonKeyedWindowSliceStoreAppendOperator::PhysicalNonKeyedWindowSliceStoreAppendOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedGlobalSliceStoreAppendOperatorHandlerPtr keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractEmitOperator(),
      windowHandler(keyedEventTimeWindowHandler) {}

std::shared_ptr<PhysicalNonKeyedWindowSliceStoreAppendOperator> PhysicalNonKeyedWindowSliceStoreAppendOperator::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedGlobalSliceStoreAppendOperatorHandlerPtr keyedEventTimeWindowHandler) {
    return std::make_shared<PhysicalNonKeyedWindowSliceStoreAppendOperator>(getNextOperatorId(),
                                                                            inputSchema,
                                                                            outputSchema,
                                                                            keyedEventTimeWindowHandler);
}

std::string PhysicalNonKeyedWindowSliceStoreAppendOperator::toString() const {
    return "PhysicalNonKeyedWindowSliceStoreAppendOperator";
}

OperatorNodePtr PhysicalNonKeyedWindowSliceStoreAppendOperator::copy() {
    return create(inputSchema, outputSchema, windowHandler);
}

}// namespace NES::QueryCompilation::PhysicalOperators
