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

#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Operators/Operator.hpp>

namespace NES
{
WindowOperator::WindowOperator(std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition, OperatorId id)
    : WindowOperator(std::move(windowDefinition), id, INVALID_ORIGIN_ID)
{
}

WindowOperator::WindowOperator(
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition, const OperatorId id, const OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId), windowDefinition(std::move(windowDefinition))
{
}

std::shared_ptr<Windowing::LogicalWindowDescriptor> WindowOperator::getWindowDefinition() const
{
    return windowDefinition;
}

std::vector<OriginId> WindowOperator::getOutputOriginIds() const
{
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

void WindowOperator::setOriginId(const OriginId originId)
{
    OriginIdAssignmentOperator::setOriginId(originId);
    windowDefinition->setOriginId(originId);
}


}
