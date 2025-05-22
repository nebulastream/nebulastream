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
#include <cstdlib>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES::Systest
{

struct FieldResult
{
    std::shared_ptr<DataType> type;
    std::string valueAsString;
};

using MapFieldNameToValue = std::unordered_map<std::string, FieldResult>;

static constexpr auto EPSILON = 1e-5;
bool operator!=(const FieldResult& left, const FieldResult& right);
bool operator==(const MapFieldNameToValue& left, const MapFieldNameToValue& right);
bool operator!=(const MapFieldNameToValue& left, const MapFieldNameToValue& right);

template <typename T>
bool compareStringAsTypeWithError(const std::string& left, const std::string& right, const double epsilonAllowed)
{
    /// We need to compare the strings as the correct type
    /// It is not possible to compare the strings directly, because the string representation of a float can be different
    /// Therefore, we need to convert the strings to the correct type and compare the values for float values
    /// We only care to cast for a float to a double
    if constexpr (std::is_floating_point_v<T>)
    {
        const auto doubleLeft = std::stod(left);
        const auto doubleRight = std::stod(right);
        const auto absDoubleLeft = std::abs(doubleLeft);
        const auto absDoubleRight = std::abs(doubleRight);
        const auto absDiff = std::abs(doubleLeft - doubleRight);

        /// Quick exit if the values are equal and also handles infinities
        if (doubleLeft == doubleRight)
        {
            return true;
        }

        /// As pointed out in https://floating-point-gui.de/errors/comparison/, we need to do an absolute difference, due the relative difference being meaningless for small values
        /// IMPORTANT: std::numeric_limits<double>::min() returns the minimum finite value representable by a double and not the smallest value
        if (doubleLeft == 0.0 || doubleRight == 0.0 || (absDoubleLeft + absDoubleRight < std::numeric_limits<double>::min()))
        {
            return absDiff < (epsilonAllowed * std::numeric_limits<double>::min());
        }

        /// If neither values is zero, close to the smallest value, we calculate the relative error
        /// IMPORTANT: std::numeric_limits<double>::max() returns the maximum finite value representable by a double and not the largest value
        const auto relativeErrorCalculated = absDiff / (std::min(absDoubleLeft + absDoubleRight, std::numeric_limits<double>::max()));
        const auto allowedError = relativeErrorCalculated < epsilonAllowed;
        if (not allowedError)
        {
            NES_TRACE(
                "Relative error {} is greater than allowed error {} for values {} and {}",
                relativeErrorCalculated,
                epsilonAllowed,
                doubleLeft,
                doubleRight);
        }
        return allowedError;
    }
    else if constexpr (std::is_same_v<T, std::string> || std::is_integral_v<T>)
    {
        const auto equal = left == right;
        return equal;
    }
    else
    {
        throw InvalidDynamicCast("Unknown type {}", typeid(T).name());
    }
}

template <typename T>
bool compareStringAsTypeWithError(const std::string& left, const std::string& right)
{
    return compareStringAsTypeWithError<T>(left, right, EPSILON);
}

struct QueryResult
{
    SystestParser::Schema fields;
    std::vector<std::string> result;
};

/// loads the query result for a sep
std::optional<QueryResult> loadQueryResult(const Query& query);

/// Returns an error message or an empty optional if the query result is correct
std::optional<std::string> checkResult(const RunningQuery& runningQuery);


}
