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

#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>

namespace NES::Statistic {

void WindowStatisticDescriptor::inferStamps(const SchemaPtr& inputSchema) {
    field->inferStamp(inputSchema);
}

WindowStatisticDescriptor::WindowStatisticDescriptor(const FieldAccessExpressionNodePtr& field, const uint64_t width)
    : field(field), width(width) {}

FieldAccessExpressionNodePtr WindowStatisticDescriptor::getField() const { return field; }

uint64_t WindowStatisticDescriptor::getWidth() const { return width; }
}// namespace NES::Statistic