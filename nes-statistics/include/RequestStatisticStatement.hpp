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

#include <string>
#include <unordered_map>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <CollectionDomain.hpp>
#include <ConditionTrigger.hpp>
#include <Metric.hpp>

namespace NES
{

struct RequestStatisticBuildStatement
{
    CollectionDomain domain;
    Metric metric;

    /// The window the statistic is aggregated over. TimeBasedWindowType already models the tumbling/sliding
    /// choice and carries size and slide as TimeMeasures, so it replaces the loose windowSizeMs/windowAdvanceMs
    /// pair: a sliding window is a SlidingWindow rather than a size plus an engaged optional, and the unit is
    /// part of the type instead of a suffix in the field name. It is also exactly what addWindowAggregation
    /// takes, so the generator now forwards it instead of reconstructing it.
    Windowing::TimeBasedWindowType windowType;

    /// How the window gets its timestamps. Defaults to ingestion time; construct an event-time characteristic
    /// over a field to use that instead. This subsumes the old optional eventTimeFieldName, and gains the time
    /// unit -- the old form silently assumed milliseconds.
    Windowing::TimeCharacteristic timeCharacteristic{
        Windowing::UnboundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createIngestionTime()}};

    /// What to report and to whom. Always present: "do not report" is NEVER_SEND, which is a condition like any
    /// other rather than a disengaged optional, and it is what decides the shape of the deployed plan. See
    /// ConditionTrigger.
    ConditionTrigger conditionTrigger{NEVER_SEND};

    std::unordered_map<std::string, std::string> options;
};

}
