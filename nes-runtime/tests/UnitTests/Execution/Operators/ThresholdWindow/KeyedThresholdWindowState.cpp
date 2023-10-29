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

#include <Execution/Operators/ThresholdWindow/KeyedThresholdWindow/KeyedThresholdWindowState.hpp>

namespace NES::Runtime::Execution::Operators {

KeyedThresholdWindowState::KeyedThresholdWindowState(const NES::Runtime::Execution::Operators::KeyedThresholdWindowState& other) {
    aggregationValues.reserve(other.aggregationValues.size());
    for (const auto& value : other.aggregationValues) {
        aggregationValues.push_back(std::make_unique<Aggregation::AggregationValue>(*value));
    }
    recordCount = other.recordCount;
    isWindowOpen = other.isWindowOpen;
}

KeyedThresholdWindowState::KeyedThresholdWindowState() = default;

}// namespace NES::Runtime::Execution::Operators