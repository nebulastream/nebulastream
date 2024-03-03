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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/Metric.hpp>
#include <utility>

namespace NES::Statistic {
Metric::Metric(FieldAccessExpressionNodePtr field) : field(std::move(field)) {}

FieldAccessExpressionNodePtr Metric::getField() const { return field; }

FieldAccessExpressionNodePtr Over(std::string name) {
    return FieldAccessExpressionNode::create(std::move(name))->as<FieldAccessExpressionNode>();
}
}//namespace NES::Statistic