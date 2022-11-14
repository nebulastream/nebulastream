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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_THRESHOLDWINDOW_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_THRESHOLDWINDOW_HPP_

#include <Windowing/Runtime/ThresholdWindowState.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/ContentBasedWindowType.hpp>

namespace NES::Windowing {

class ThresholdWindow : public ContentBasedWindowType {
  public:
    static WindowTypePtr of(ExpressionNodePtr predicate);

    static WindowTypePtr of(ExpressionNodePtr predicate, uint64_t miniumCount);

    /**
    * @brief Returns true, because this a threshold window
    * @return true
    */
    bool isThresholdWindow() override;

    std::string toString() override;

    bool equal(WindowTypePtr otherWindowType) override;

    [[nodiscard]] const ExpressionNodePtr& getPredicate() const;

    uint64_t getMiniumCount();

    /**
     * @brief Triggers windows if they marked as closed
     * @param windows vector of windows
     */
    void triggerWindows(std::vector<ThresholdWindowState>& windows);

  private:
    explicit ThresholdWindow(ExpressionNodePtr predicate);
    ThresholdWindow(ExpressionNodePtr predicate, uint64_t minCount);

    ExpressionNodePtr predicate;
    uint64_t miniumCount = 0;
    bool isClosed = false;
};

}// namespace NES::Windowing

#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_THRESHOLDWINDOW_HPP_
