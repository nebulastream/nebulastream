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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_KEYEDTHRESHOLDWINDOW_KEYEDTHRESHOLDWINDOWSTATE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_KEYEDTHRESHOLDWINDOW_KEYEDTHRESHOLDWINDOWSTATE_HPP_

#include <mutex>

namespace NES::Runtime::Execution::Operators {
/**
 * @brief This class encapsulate a state for a KeyedThresholdWindow.
 * This state belongs to a single key, and can contain multiple aggregationValues (i.e., one for each
 * aggregation function)
 */
class KeyedThresholdWindowState {
  public:
    KeyedThresholdWindowState();

    KeyedThresholdWindowState(const KeyedThresholdWindowState& other);

    std::vector<std::unique_ptr<Aggregation::AggregationValue>> aggregationValues{};
    uint64_t recordCount = 0;// counts the records contributing to the aggregate,
    bool isWindowOpen = false;
    std::mutex mutex;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_KEYEDTHRESHOLDWINDOW_KEYEDTHRESHOLDWINDOWSTATE_HPP_
