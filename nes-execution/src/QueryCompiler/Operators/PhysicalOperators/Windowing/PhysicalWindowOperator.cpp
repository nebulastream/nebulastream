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
#include <utility>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalWindowOperator::PhysicalWindowOperator(
    OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::LogicalWindowDescriptorPtr windowDefinition)
    : Operator(id)
    , PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema))
    , windowDefinition(std::move(windowDefinition))
{
}

const Windowing::LogicalWindowDescriptorPtr& PhysicalWindowOperator::getWindowDefinition() const
{
    return windowDefinition;
};

std::string PhysicalWindowOperator::toString() const
{
    return PhysicalUnaryOperator::toString();
}

}
