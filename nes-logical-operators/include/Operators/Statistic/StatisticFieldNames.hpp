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
#include <string_view>

#include <Identifiers/StatisticIdentifiers.hpp>
#include <fmt/format.h>

namespace NES
{

/// The field names a statistic build chain agrees on, as plain strings.
namespace StatisticFieldNames
{
inline constexpr std::string_view NUMBER_OF_SEEN_TUPLES = "STATISTICNUMBEROFSEENTUPLES";
inline constexpr std::string_view NUMBER_OF_SEEN_MEASUREMENTS = "STATISTICNUMBEROFSEENMEASUREMENTS";
inline constexpr std::string_view STATISTIC_ID = "STATISTICID";
inline constexpr std::string_view START_TS = "STATISTICSTART";
inline constexpr std::string_view END_TS = "STATISTICEND";
/// The read side only: the scalar value a probe reconstructs from a stored statistic.
inline constexpr std::string_view VALUE = "STATISTICVALUE";
}

inline std::string statisticDataFieldName(const StatisticId statisticId)
{
    return fmt::format("STATISTICDATA_{}", statisticId.getRawValue());
}

}
