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
#include <Common/DataTypes/Boolean.hpp>

namespace NES::Windowing
{

ThresholdWindow::ThresholdWindow(LogicalFunction predicate) : ContentBasedWindowType(), predicate(std::move(predicate))
{
}

ThresholdWindow::ThresholdWindow(LogicalFunction predicate, uint64_t minCount)
    : ContentBasedWindowType(), predicate(std::move(predicate)), minimumCount(minCount)
{
}

bool ThresholdWindow::operator==(const WindowType& otherWindowType) const
{
    if (auto other = dynamic_cast<const ThresholdWindow*>(&otherWindowType))
    {
        return this->minimumCount == other->minimumCount && this->predicate == other->predicate;
    }
    return false;
}

ContentBasedWindowType::ContentBasedSubWindowType ThresholdWindow::getContentBasedSubWindowType()
{
    return ContentBasedSubWindowType::THRESHOLDWINDOW;
}

const LogicalFunction ThresholdWindow::getPredicate() const
{
    return predicate;
}

uint64_t ThresholdWindow::getMinimumCount() const
{
    return minimumCount;
}

bool ThresholdWindow::inferStamp(const Schema& schema)
{
    auto newPredicate = predicate.withInferredStamp(schema);
    INVARIANT(*newPredicate.getStamp().get() == Boolean(), "the threshold function is not a valid predicate");
    this->predicate = newPredicate;
    return true;
}

std::string ThresholdWindow::toString() const
{
    std::stringstream ss;
    ss << "Threshold Window: predicate ";
    ss << predicate;
    ss << "and minimumCount";
    ss << minimumCount;
    ss << '\n';
    return ss.str();
}
}
