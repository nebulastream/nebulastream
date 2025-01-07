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
#include <sstream>
#include <utility>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalOperatorPtr PhysicalSliceMergingOperator::create(
    OperatorId id,
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    const Windowing::LogicalWindowDescriptorPtr& windowDefinition)
{
    return std::make_shared<PhysicalSliceMergingOperator>(id, inputSchema, outputSchema, windowDefinition);
}

PhysicalSliceMergingOperator::PhysicalSliceMergingOperator(
    OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::LogicalWindowDescriptorPtr windowDefinition)
    : Operator(id), PhysicalWindowOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(windowDefinition)) {};

std::string PhysicalSliceMergingOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalSliceMergingOperator:\n";
    out << PhysicalWindowOperator::toString();
    return out.str();
}

std::shared_ptr<Operator> PhysicalSliceMergingOperator::copy()
{
    return create(id, inputSchema, outputSchema, windowDefinition);
}

}
