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

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <utility>
#include <API/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Types/ContentBasedWindowType.hpp>
#include <WindowTypes/Types/ThresholdWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{

ThresholdWindow::ThresholdWindow(std::shared_ptr<NodeFunction> predicate) : predicate(std::move(predicate))
{
}

ThresholdWindow::ThresholdWindow(std::shared_ptr<NodeFunction> predicate, uint64_t minCount)
    : predicate(std::move(predicate)), minimumCount(minCount)
{
}

std::shared_ptr<WindowType> ThresholdWindow::of(std::shared_ptr<NodeFunction> predicate)
{
    return std::reinterpret_pointer_cast<WindowType>(std::make_shared<ThresholdWindow>(ThresholdWindow(std::move(predicate))));
}

std::shared_ptr<WindowType> ThresholdWindow::of(std::shared_ptr<NodeFunction> predicate, uint64_t minimumCount)
{
    return std::reinterpret_pointer_cast<WindowType>(
        std::make_shared<ThresholdWindow>(ThresholdWindow(std::move(predicate), minimumCount)));
}

bool ThresholdWindow::equal(std::shared_ptr<WindowType> otherWindowType)
{
    if (auto otherThresholdWindow = std::dynamic_pointer_cast<ThresholdWindow>(otherWindowType))
    {
        return this->minimumCount == otherThresholdWindow->minimumCount && this->predicate == otherThresholdWindow->predicate;
    }
    return false;
}

ContentBasedWindowType::ContentBasedSubWindowType ThresholdWindow::getContentBasedSubWindowType()
{
    return ContentBasedSubWindowType::THRESHOLDWINDOW;
}

const std::shared_ptr<NodeFunction>& ThresholdWindow::getPredicate() const
{
    return predicate;
}

uint64_t ThresholdWindow::getMinimumCount() const
{
    return minimumCount;
}

bool ThresholdWindow::inferStamp(const Schema& schema)
{
    NES_INFO("inferStamp for ThresholdWindow")
    predicate->inferStamp(schema);
    INVARIANT(predicate->isPredicate(), "the threshold function is not a valid predicate");
    return true;
}

std::string ThresholdWindow::toString() const
{
    std::stringstream ss;
    ss << "Threshold Window: predicate ";
    ss << *predicate;
    ss << "and minimumCount";
    ss << minimumCount;
    ss << '\n';
    return ss.str();
}
}
