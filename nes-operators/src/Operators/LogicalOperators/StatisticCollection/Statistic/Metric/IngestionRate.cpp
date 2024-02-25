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

#include <Operators/LogicalOperators/StatisticCollection/Statistic/Metric/IngestionRate.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>

namespace NES::Statistic {

MetricPtr IngestionRate::create() { return std::make_shared<IngestionRate>(IngestionRate()); }

IngestionRate::IngestionRate() : Metric(FieldAccessExpressionNode::create(INGESTION_RATE_FIELD_NAME)->as<FieldAccessExpressionNode>()) {}

bool IngestionRate::operator==(const Metric& rhs) const {
    if (rhs.instanceOf<IngestionRate>()) {
        auto rhsIngestionRate = dynamic_cast<const IngestionRate&>(rhs);
        return field == rhsIngestionRate.field;
    }
    return false;
}

std::string IngestionRate::toString() const { return "IngestionRate"; }

}// namespace NES::Statistic