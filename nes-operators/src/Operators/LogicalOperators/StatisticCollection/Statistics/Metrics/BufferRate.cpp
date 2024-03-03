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

#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/BufferRate.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>

namespace NES::Statistic {

MetricPtr BufferRate::create() { return std::make_shared<BufferRate>(BufferRate()); }

bool BufferRate::operator==(const Metric& rhs) const {
    if (rhs.instanceOf<BufferRate>()) {
        auto rhsBufferRate = dynamic_cast<const BufferRate&>(rhs);
        return field->equal(rhsBufferRate.field);
    }
    return false;
}

BufferRate::BufferRate() : Metric(FieldAccessExpressionNode::create(BUFFER_RATE_FIELD_NAME)->as<FieldAccessExpressionNode>()) {}

std::string BufferRate::toString() const { return "BufferRate"; }

}// namespace NES::Statistic