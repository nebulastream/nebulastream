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

#include <functional>
#include <optional>
#include <Functions/LogicalFunction.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <Statistic.hpp>

namespace NES
{

/// A condition-callback pair. When a statistic result arrives the callback fires.
/// If condition is set, only results satisfying it trigger the callback.
/// If condition is std::nullopt, the callback fires unconditionally on every result.
struct ConditionTrigger
{
    std::optional<LogicalFunction> condition;
    std::function<void(Statistic::StatisticId, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs)> callback;
};

}
