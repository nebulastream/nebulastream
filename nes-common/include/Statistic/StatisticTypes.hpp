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

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <Identifiers/StatisticIdentifiers.hpp>

/// The identity and kind of a statistic, split out of the 'StatisticTuple' value class in nes-statistics so that
/// nes-logical-operators can name a statistic without depending on nes-statistics.
///
/// The full 'StatisticTuple' carries 'Windowing::TimeMeasure', which lives in nes-logical-operators; moving all of
/// it down here would point that dependency the wrong way. Operators and aggregation functions only ever need
/// the id and the type, so only those live here. 'StatisticTuple' aliases both back, which keeps every
/// 'StatisticTuple::StatisticId' / 'StatisticTuple::StatisticType' spelling working unchanged.
namespace NES
{

/// Defines what statistic type is held in the underlying statistic memory area.
///
/// Only the scalar kinds exist here: statistic-store-backed aggregations whose synopsis is a single scalar.
/// The synopsis kinds (equi-width histogram, reservoir sample, count-min sketch) are deliberately not ported.
enum class StatisticType : uint8_t
{
    Count,
    Sum,
    Avg
};

/// The field names a statistic build chain agrees on, as plain strings.
///
/// Both layers need these and they cannot live in either one: the logical layer wraps them in unbound fields
/// (see LogicalStatisticFields), while the physical layer writes them into Nautilus records as bare
/// Record::RecordFieldIdentifier strings, and nes-physical-operators must not depend on nes-logical-operators.
namespace StatisticFieldNames
{
inline constexpr std::string_view NUMBER_OF_SEEN_TUPLES = "STATISTICNUMBEROFSEENTUPLES";
inline constexpr std::string_view STATISTIC_ID = "STATISTICID";
inline constexpr std::string_view START_TS = "STATISTICSTART";
inline constexpr std::string_view END_TS = "STATISTICEND";
inline constexpr std::string_view DATA = "STATISTICDATA";
inline constexpr std::string_view TYPE = "STATISTICTYPE";
/// The read side only: the scalar value a probe reconstructs from a stored statistic.
inline constexpr std::string_view VALUE = "STATISTICVALUE";
/// Size of the persisted payload. Reported by a build query so its aggregation stays referenced -- see
/// DefaultStatisticQueryGenerator for why that matters.
inline constexpr std::string_view PAYLOAD_BYTES = "STATISTICPAYLOADBYTES";
}

}
