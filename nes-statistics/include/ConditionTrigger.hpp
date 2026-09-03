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

#include <algorithm>
#include <cctype>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <StatisticTuple.hpp>

namespace NES
{

/// A condition-callback pair, and the single knob that decides what a statistic request actually deploys.
///
/// The condition is SQL rather than a LogicalFunction: it is what a caller can write without assembling a
/// function tree, and DefaultStatisticQueryGenerator parses it exactly once, when it builds the plan. It is
/// evaluated over the probed statistic's columns -- STATISTICID, STATISTICSTART, STATISTICEND, STATISTICVALUE --
/// so "STATISTICVALUE > 100" is the shape to expect.
///
/// Two conditions are recognised structurally rather than parsed, because they say something about the *plan*
/// and not about a row: NEVER_SEND and ALWAYS_SEND below. Every other condition becomes a Selection.
struct ConditionTrigger
{
    /// Fired for each reported window, with the statistic's value.
    using Callback
        = std::function<void(StatisticTuple::StatisticId, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs, double value)>;

    /// The two conditions that are matched literally. Spelled as the SQL constants they are, so that a caller who
    /// writes "true" by hand lands in the same plan shape as one who uses ALWAYS_SEND.
    static constexpr std::string_view NEVER_CONDITION = "false";
    static constexpr std::string_view ALWAYS_CONDITION = "true";

    std::string condition;
    Callback callback;

    /// Case 1: nothing is ever reported, so the query needs neither a probe nor a network sink. An empty
    /// condition counts as never: a default-constructed trigger asks for nothing, and reading it as "no filter,
    /// report everything" would be the more surprising of the two readings.
    [[nodiscard]] bool sendsNever() const
    {
        const auto normalised = normalisedCondition();
        return normalised.empty() or normalised == NEVER_CONDITION;
    }

    /// Case 2: every closed window is reported, so the query needs the probe but no Selection.
    [[nodiscard]] bool sendsAlways() const { return normalisedCondition() == ALWAYS_CONDITION; }

    /// Matching a hand-written condition against the two sentinels is a string comparison, so it has to agree with
    /// the caller on spelling. Case and surrounding space are the two ways "true" is written that SQL would treat
    /// as identical, so both are normalised away before comparing; anything else is left for the parser.
    [[nodiscard]] std::string normalisedCondition() const
    {
        const auto begin = condition.find_first_not_of(" \t\n\r\f\v");
        if (begin == std::string::npos)
        {
            return {};
        }
        const auto end = condition.find_last_not_of(" \t\n\r\f\v");
        auto trimmed = condition.substr(begin, end - begin + 1);
        std::ranges::transform(
            trimmed, trimmed.begin(), [](const unsigned char character) { return static_cast<char>(std::tolower(character)); });
        return trimmed;
    }
};

/// Terminates the build query in a VoidSink: the statistic is still persisted, and getStatistics reads it back.
/// This is the default -- a request that says nothing about reporting does not report.
inline const ConditionTrigger NEVER_SEND{std::string{ConditionTrigger::NEVER_CONDITION}, nullptr};

/// Reports every closed window. Only useful with a callback attached, see withCallback.
inline const ConditionTrigger ALWAYS_SEND{std::string{ConditionTrigger::ALWAYS_CONDITION}, nullptr};

/// Attaches a callback to a condition: `withCallback(ALWAYS_SEND, cb)` or `withCallback({"STATISTICVALUE > 100"}, cb)`.
inline ConditionTrigger withCallback(ConditionTrigger trigger, ConditionTrigger::Callback callback)
{
    trigger.callback = std::move(callback);
    return trigger;
}

}
