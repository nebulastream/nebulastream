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

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <CollectionDomain.hpp>
#include <ConditionTrigger.hpp>
#include <Metric.hpp>

namespace NES
{

struct RequestStatisticBuildStatement
{
    CollectionDomain domain;
    Metric metric;
    uint64_t windowSizeMs;
    std::optional<uint64_t> windowAdvanceMs;
    /// If set, the window uses EventTime on this field. Otherwise IngestionTime is used.
    std::optional<std::string> eventTimeFieldName;
    /// Optional filter predicate applied to the statistic result. Used by the trigger system.
    std::optional<ConditionTrigger> conditionTrigger;
    std::unordered_map<std::string, std::string> options;
};

}
