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

#include <Operators/Expressions/ExpressionNode.hpp>
#include <StatisticCollection/Statistic/Metric/Selectivity.hpp>

namespace NES::Statistic {
MetricPtr Selectivity::create(const ExpressionNodePtr& expressionNode) {
    return std::make_shared<Selectivity>(Selectivity(expressionNode));
}

bool Selectivity::operator==(const Metric& rhs) const {
    if (rhs.instanceOf<Selectivity>()) {
        auto rhsSelectivity = dynamic_cast<const Selectivity&>(rhs);
        return expressionNode->equal(rhsSelectivity.expressionNode);
    }
    return false;
}

Selectivity::Selectivity(const ExpressionNodePtr& expressionNode) : expressionNode(expressionNode) {}

const ExpressionNodePtr& Selectivity::getExpressionNode() const { return expressionNode; }
}// namespace NES::Statistic