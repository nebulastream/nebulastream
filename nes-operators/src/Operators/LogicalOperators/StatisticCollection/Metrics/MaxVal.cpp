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

#include <Operators/LogicalOperators/StatisticCollection/Metrics/MaxVal.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>

namespace NES::Statistic {

MetricPtr MaxVal::create(const FieldAccessExpressionNodePtr& field) { return std::make_shared<MaxVal>(MaxVal(field)); }

MaxVal::MaxVal(const FieldAccessExpressionNodePtr& field) : StatisticMetric(field) {}

bool MaxVal::operator==(const StatisticMetric& rhs) const {
    if (rhs.instanceOf<MaxVal>()) {
        // We assume that if the field has the same name, the metric is equal
        auto rhsMaxVal = dynamic_cast<const MaxVal&>(rhs);
        return field->getFieldName() == rhsMaxVal.field->getFieldName();
    }
    return false;
}

std::string MaxVal::toString() const { return "MaxVal over " + field->toString(); }
}// namespace NES::Statistic