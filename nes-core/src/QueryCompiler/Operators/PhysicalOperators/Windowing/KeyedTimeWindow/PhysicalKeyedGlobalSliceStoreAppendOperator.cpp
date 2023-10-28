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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedGlobalSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedGlobalSliceStoreAppendOperatorHandler.hpp>
#include <memory>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalKeyedGlobalSliceStoreAppendOperator::PhysicalKeyedGlobalSliceStoreAppendOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    std::shared_ptr<Windowing::Experimental::KeyedGlobalSliceStoreAppendOperatorHandler> keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractEmitOperator(),
      keyedEventTimeWindowHandler(keyedEventTimeWindowHandler) {}

std::shared_ptr<PhysicalKeyedGlobalSliceStoreAppendOperator> PhysicalKeyedGlobalSliceStoreAppendOperator::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::KeyedGlobalSliceStoreAppendOperatorHandlerPtr keyedEventTimeWindowHandler) {
    return std::make_shared<PhysicalKeyedGlobalSliceStoreAppendOperator>(getNextOperatorId(),
                                                                         inputSchema,
                                                                         outputSchema,
                                                                         keyedEventTimeWindowHandler);
}

std::string PhysicalKeyedGlobalSliceStoreAppendOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalKeyedGlobalSliceStoreAppendOperator:\n";
    out << PhysicalUnaryOperator::toString();
    out << keyedEventTimeWindowHandler->toString();
    return out.str();
}

OperatorNodePtr PhysicalKeyedGlobalSliceStoreAppendOperator::copy() {
    return create(inputSchema, outputSchema, keyedEventTimeWindowHandler);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES