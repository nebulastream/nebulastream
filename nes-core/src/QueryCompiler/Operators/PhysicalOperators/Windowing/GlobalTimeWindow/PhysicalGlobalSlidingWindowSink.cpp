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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <memory>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalGlobalSlidingWindowSink::PhysicalGlobalSlidingWindowSink(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalSlidingWindowSinkOperatorHandlerPtr keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractScanOperator(),
      keyedEventTimeWindowHandler(keyedEventTimeWindowHandler) {}

std::shared_ptr<PhysicalGlobalSlidingWindowSink> PhysicalGlobalSlidingWindowSink::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalSlidingWindowSinkOperatorHandlerPtr keyedEventTimeWindowHandler) {
    return std::make_shared<PhysicalGlobalSlidingWindowSink>(Util::getNextOperatorId(),
                                                             inputSchema,
                                                             outputSchema,
                                                             keyedEventTimeWindowHandler);
}

std::string PhysicalGlobalSlidingWindowSink::toString() const { return "PhysicalGlobalSlidingWindowSink"; }

OperatorNodePtr PhysicalGlobalSlidingWindowSink::copy() { return create(inputSchema, outputSchema, keyedEventTimeWindowHandler); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES