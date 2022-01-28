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
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr
PhysicalWindowSinkOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WindowOperatorHandlerPtr handler) {
    return create(Util::getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(handler));
}

PhysicalOperatorPtr PhysicalWindowSinkOperator::create(OperatorId id,
                                                       const SchemaPtr& inputSchema,
                                                       const SchemaPtr& outputSchema,
                                                       const Windowing::WindowOperatorHandlerPtr& handler) {
    return std::make_shared<PhysicalWindowSinkOperator>(id, inputSchema, outputSchema, handler);
}

PhysicalWindowSinkOperator::PhysicalWindowSinkOperator(OperatorId id,
                                                       SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       Windowing::WindowOperatorHandlerPtr handler)
    : OperatorNode(id), PhysicalWindowOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(handler)){};

std::string PhysicalWindowSinkOperator::toString() const { return "PhysicalWindowSinkOperator"; }

OperatorNodePtr PhysicalWindowSinkOperator::copy() { return create(id, inputSchema, outputSchema, operatorHandler); }

}// namespace NES::QueryCompilation::PhysicalOperators