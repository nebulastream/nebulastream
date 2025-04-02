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
#include <algorithm>
#include <charconv>
#include <cstdint>
#include <expected>
#include <iostream>
#include <map>
#include <optional>
#include <random>
#include <ranges>
#include <string>
#include <tuple>
#include <vector>
#include <Execution/Operators/Streaming/Aggregation/Function/Synopsis/Sample/ReservoirSampleFunction.hpp>
/// TODO(nmlt) Fix CMake to include Logger.h
///#include <Util/Logger/Logger.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

/// Per Window Reservoir
class WindowReservoir
{
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> reservoir;
    uint64_t index = 0;

public:
    void emplaceBack(std::tuple<uint64_t, uint64_t, uint64_t> tuple)
    {
        reservoir.emplace_back(tuple);
        ++index;
    }
    void replace(uint64_t index, std::tuple<uint64_t, uint64_t, uint64_t> tuple) { reservoir.at(index) = tuple; }
    void increaseIndex() { ++index; }
    [[nodiscard]] uint64_t getIndex() const { return index; }
    [[nodiscard]] uint64_t size() const { return reservoir.size(); }
    [[nodiscard]] std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> getReservoir() const { return reservoir; }
};

/// Hardcoded values to be changed for different queries:
/// We expect: id,value,timestamp in msec
const std::string input = R"(1,1,1000
12,1,1001
4,1,1002
1,2,2000
11,2,2001
16,2,2002
1,3,3000
11,3,3001
1,3,3003
1,3,3200
1,4,4000
1,5,5000
1,6,6000
1,7,7000
1,8,8000
1,9,9000
1,10,10000
1,11,11000
1,12,12000
1,13,13000
1,14,14000
1,15,15000
1,16,16000
1,17,17000
1,18,18000
1,19,19000
1,20,20000
1,21,21000)";

struct Reservoir
{
    std::map<uint64_t, WindowReservoir> map;
    /// Number of tuples in the reservoir
    const uint64_t reservoirSize = 5;
    /// Size of the window in milliseconds
    const uint64_t windowSizeMSec = 5000;

    std::optional<uint64_t> findCorrespondingWindow(uint64_t ts)
    {
        auto it = std::find_if(
            map.begin(),
            map.end(),
            [&](const auto& pair)
            {
                auto key = pair.first;
                return ts >= key && ts < key + windowSizeMSec;
            });
        if (it != map.end())
        {
            return it->first;
        }
        return std::nullopt;
    }
    size_t getReservoirSize(uint64_t ts)
    {
        auto key = findCorrespondingWindow(ts);
        if (key.has_value() && map.contains(key.value()))
        {
            return map.at(key.value()).size();
        }
        return 0;
    }
};

/// std::string formatTuple(const std::tuple<uint64_t, uint64_t, uint64_t>& tuple)
/// {
///     return fmt::format("({}, {}, {})", std::get<0>(tuple), std::get<1>(tuple), std::get<2>(tuple));
/// }

std::optional<std::tuple<uint64_t, uint64_t, uint64_t>> parseLine(const std::string_view& line)
{
    auto fields = line | std::views::split(',');
    auto fieldsIt = fields.begin();
    auto idString = std::string_view(*fieldsIt++);
    auto valueString = std::string_view(*fieldsIt++);
    auto timestampString = std::string_view(*fieldsIt);

    uint64_t id;
    uint64_t value;
    uint64_t timestamp;
    auto result = std::from_chars(idString.data(), idString.data() + idString.size(), id);
    if (result.ec == std::errc::invalid_argument || result.ec == std::errc::result_out_of_range)
    {
        NES_ERROR("Failed to convert field id");
        return std::nullopt;
    }
    result = std::from_chars(valueString.data(), valueString.data() + valueString.size(), value);
    if (result.ec == std::errc::invalid_argument || result.ec == std::errc::result_out_of_range)
    {
        NES_ERROR("Failed to convert field value");
        return std::nullopt;
    }
    result = std::from_chars(timestampString.data(), timestampString.data() + timestampString.size(), timestamp);
    if (result.ec == std::errc::invalid_argument || result.ec == std::errc::result_out_of_range)
    {
        NES_ERROR("Failed to convert field timestamp");
        return std::nullopt;
    }

    return std::make_tuple(id, value, timestamp);
}

/// Simulate a reservoir sample over a tumbling window
int main()
{
    auto reservoir = Reservoir();
    for (auto line : input | std::views::split('\n'))
    {
        //std::cout << std::string_view(line.begin(), line.end()) << '\n';
        auto parsedTuple = parseLine(std::string_view(line.begin(), line.end()));
        if (not parsedTuple.has_value())
        {
            NES_ERROR("Failed to parse line: `{}`", line);
            return 1;
        }
        auto tuple = parsedTuple.value();
        auto timestamp = std::get<2>(tuple);

        /// Probably still assumes timestamps are ordered at some point in the code
        uint64_t lowerBound = 0;
        uint64_t upperBound = lowerBound + reservoir.windowSizeMSec;
        /// `lowerBound` (current window) has overtaken input
        if (timestamp < lowerBound)
        {
            /// Go back in the map until we find the window this tuple belongs to
            auto key = reservoir.findCorrespondingWindow(timestamp);
            if (key.has_value())
            {
                lowerBound = key.value();
            }
            else
            {
                NES_ERROR("Failed to find older window.");
                return 1;
            }
        }
        /// New window necessary
        else if (timestamp >= upperBound)
        {
            while (timestamp >= upperBound)
            {
                upperBound += reservoir.windowSizeMSec;
            }
            lowerBound = upperBound - reservoir.windowSizeMSec;
        }
        /// else we are still in the same window: timestamp >= lowerBound && timestamp < upperBound

        /// Reservoir is still not full
        if (reservoir.getReservoirSize(lowerBound) < reservoir.reservoirSize)
        {
            reservoir.map.try_emplace(lowerBound).first->second.emplaceBack(tuple);
        }
        /// Draw to find out if we need to replace a tuple in the reservoir
        else
        {
            static std::mt19937 gen(NES::Runtime::Execution::Aggregation::Synopsis::ReservoirSampleFunction::GENERATOR_SEED);
            uint64_t currentIndex = reservoir.map.at(lowerBound).getIndex();
            std::uniform_int_distribution<> dis(0, static_cast<int>(currentIndex));
            uint64_t random = dis(gen);
            NES_DEBUG("We generated a random number!");
            if (random < reservoir.reservoirSize)
            {
                NES_DEBUG("The random number was < reservoirSize!");
                reservoir.map.try_emplace(lowerBound).first->second.replace(random, tuple);
            }
            reservoir.map.at(lowerBound).increaseIndex();
        }
    }

    std::cout << "window start, window end, [reservoir sample]\n";
    for (const auto& [key, value] : reservoir.map)
    {
        ///std::cout << fmt::format("{}, {}, [{}]\n", key, key + reservoir.windowSizeMSec, fmt::join(value.getReservoir() | std::views::transform(formatTuple), ", "));
        for (auto tuples : value.getReservoir())
        {
            std::cout << fmt::format(
                "{}, {}, ({}, {}, {})\n",
                key,
                key + reservoir.windowSizeMSec,
                std::get<0>(tuples),
                std::get<1>(tuples),
                std::get<2>(tuples));
        }
    }
    return 0;
}
