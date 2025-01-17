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

#include <string_view>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Operators/AbstractOperators/OriginIdAssignmentOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Operators/Operator.hpp>

namespace NES
{

WindowOperator::WindowOperator(Windowing::LogicalWindowDescriptorPtr windowDefinition, OperatorId id)
    : WindowOperator(std::move(windowDefinition), id, INVALID_ORIGIN_ID)
{
}

WindowOperator::WindowOperator(Windowing::LogicalWindowDescriptorPtr windowDefinition, const OperatorId id, const OriginId originId)
    : Operator(id), LogicalUnaryOperator(id), OriginIdAssignmentOperator(id, originId), windowDefinition(std::move(windowDefinition))
{
}

Windowing::LogicalWindowDescriptorPtr WindowOperator::getWindowDefinition() const
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


void WindowOperator::setWindowStartEndKeyFieldName(const std::string_view windowStartFieldName, const std::string_view windowEndFieldName)
{
    WindowOperator::windowStartFieldName = windowStartFieldName;
    WindowOperator::windowEndFieldName = windowEndFieldName;
}

std::string WindowOperator::getWindowStartFieldName() const
{
    return windowStartFieldName;
}

std::string WindowOperator::getWindowEndFieldName() const
{
    return windowEndFieldName;
}

}
