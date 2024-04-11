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
#include <API/Schema.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/StatisticMetric.hpp>
#include <utility>

namespace NES::Statistic {
StatisticMetric::StatisticMetric(FieldAccessExpressionNodePtr field) : field(std::move(field)) {}

FieldAccessExpressionNodePtr StatisticMetric::getField() const { return field; }

StatisticMetricHash StatisticMetric::hash() const {
    // We do not want to hash the qualifierSourceName, only the field name without it
    auto fieldNameToHash = field->getFieldName();
    if (field->getFieldName().find(Schema::ATTRIBUTE_NAME_SEPARATOR)) {
        fieldNameToHash = fieldNameToHash.substr(fieldNameToHash.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    }
    return std::hash<std::string>()(fieldNameToHash);
}

bool StatisticMetric::equal(const StatisticMetric& rhs) const { return *this == rhs; }

FieldAccessExpressionNodePtr Over(std::string name) {
    return FieldAccessExpressionNode::create(std::move(name))->as<FieldAccessExpressionNode>();
}
}//namespace NES::Statistic