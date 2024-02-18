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

#include <StatisticCollection/Statistic/Metric/BufferRate.hpp>

namespace NES::Statistic {

MetricPtr BufferRate::create() { return std::make_shared<BufferRate>(BufferRate()); }

bool BufferRate::operator==(const Metric& rhs) const {
    if (rhs.instanceOf<BufferRate>()) {
        auto rhsBufferRate = dynamic_cast<const BufferRate&>(rhs);
        return fieldName == rhsBufferRate.fieldName;
    }
    return false;
}

BufferRate::BufferRate() : fieldName(BUFFER_RATE_FIELD_NAME) {}

const std::string& BufferRate::getFieldName() const { return fieldName; }

}// namespace NES::Statistic