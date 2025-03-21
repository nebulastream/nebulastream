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

#pragma once

#include <Abstract/LogicalFunction.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/ContentBasedWindowType.hpp>

namespace NES::Windowing
{

/// Threshold window creates a window whenever an event attribute exceeds a threshold (predicate), and close the window if it is below the threshold (or the other way around)
/// Threshold windows are content based, non-overlapping windows with gaps
class ThresholdWindow : public ContentBasedWindowType
{
public:
    explicit ThresholdWindow(LogicalFunction predicate);
    ThresholdWindow(LogicalFunction predicate, uint64_t minCount);

    /// @brief Constructor for ThresholdWindow
    /// @param predicate the filter predicate of the window, if true tuple belongs to window if false not, first occurance of true starts the window, first occurance of false closes it
    /// @return std::shared_ptr<WindowType>
    static std::unique_ptr<WindowType> of(LogicalFunction predicate);

    /// @brief Constructor for ThresholdWindow
    /// @param predicate the filter predicate of the window, if true tuple belongs to window if false not, first occurance of true starts the window, first occurance of false closes it
    /// @param minimumCount specifies the minimum amount of tuples to occur within the window
    /// @return std::shared_ptr<WindowType>
    static std::unique_ptr<WindowType> of(LogicalFunction predicate, uint64_t minimumCount);

    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] bool operator==(const WindowType& otherWindowType) override;
    [[nodiscard]] std::unique_ptr<WindowType> clone() const override;

    /// @brief return the content-based Subwindow Type, i.e., THRESHOLDWINDOW
    /// @return enum content-based Subwindow Type
    ContentBasedSubWindowType getContentBasedSubWindowType() override;

    [[nodiscard]] const LogicalFunction getPredicate() const;
    [[nodiscard]] uint64_t getMinimumCount() const;
    [[nodiscard]] bool inferStamp(const Schema& schema) override;
    [[nodiscard]] uint64_t hash() const override;

private:
    LogicalFunction predicate;
    uint64_t minimumCount = 0;
};
}
