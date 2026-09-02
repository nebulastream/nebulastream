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

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <fmt/format.h>

namespace NES
{

inline std::string statisticDataFieldName(const StatisticId statisticId)
{
    return fmt::format("STATISTICDATA_{}", statisticId.getRawValue());
}

struct LogicalStatisticFields
{
    UnqualifiedUnboundField statisticNumberOfSeenMeasurementsField{
        Identifier::parse("STATISTICNUMBEROFSEENMEASUREMENTS"), DataType::Type::UINT64};
    UnqualifiedUnboundField statisticStartTsField{Identifier::parse("STATISTICSTART"), DataType::Type::UINT64};
    UnqualifiedUnboundField statisticEndTsField{Identifier::parse("STATISTICEND"), DataType::Type::UINT64};

    LogicalStatisticFields() = default;

    bool operator==(const LogicalStatisticFields&) const = default;
};

}
