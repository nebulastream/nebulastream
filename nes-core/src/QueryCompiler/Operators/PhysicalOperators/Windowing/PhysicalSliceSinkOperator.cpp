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
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr
PhysicalSliceSinkOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WindowOperatorHandlerPtr handler) {
    return create(Util::getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(handler));
}

PhysicalOperatorPtr PhysicalSliceSinkOperator::create(OperatorId id,
                                                      const SchemaPtr& inputSchema,
                                                      const SchemaPtr& outputSchema,
                                                      const Windowing::WindowOperatorHandlerPtr& handler) {
    return std::make_shared<PhysicalSliceSinkOperator>(id, inputSchema, outputSchema, handler);
}

PhysicalSliceSinkOperator::PhysicalSliceSinkOperator(OperatorId id,
                                                     SchemaPtr inputSchema,
                                                     SchemaPtr outputSchema,
                                                     Windowing::WindowOperatorHandlerPtr handler)
    : OperatorNode(id), PhysicalWindowOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(handler)){};

std::string PhysicalSliceSinkOperator::toString() const { return "PhysicalSliceSinkOperator"; }

OperatorNodePtr PhysicalSliceSinkOperator::copy() { return create(id, inputSchema, outputSchema, operatorHandler); }

}// namespace NES::QueryCompilation::PhysicalOperators