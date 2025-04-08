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
#include <iostream>
#include <map>
#include <optional>
#include <random>
#include <cctype>
#include <ranges>
#include <string_view>
#include <fstream>
#include <string>
#include <tuple>
#include <vector>
#include <Execution/Operators/Streaming/Aggregation/Function/Synopsis/Sample/ReservoirSampleFunction.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

/// Per Window Reservoir
class WindowReservoir
{
private:
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> reservoir;
    /// How many records have already passed through the window (regardless of whether they were added to the reservoir or not)
    uint64_t currRecordIdx = 0;

public:
    void emplaceBack(std::tuple<uint64_t, uint64_t, uint64_t> record)
    {
        reservoir.emplace_back(record);
        ++currRecordIdx;
    }
    void replace(uint64_t index, std::tuple<uint64_t, uint64_t, uint64_t> record) { reservoir.at(index) = record; }
    void increaseIndex() { ++currRecordIdx; }
    [[nodiscard]] uint64_t getIndex() const { return currRecordIdx; }
    [[nodiscard]] uint64_t size() const { return reservoir.size(); }
    [[nodiscard]] std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> getReservoir() const { return reservoir; }
};

struct ReservoirSampleStore
{
    ReservoirSampleStore(const uint64_t reservoirSize, const uint64_t windowSizeMSec)
        : reservoirSize(reservoirSize), windowSizeMSec(windowSizeMSec)
    {
    }
    std::optional<uint64_t> findCorrespondingWindow(uint64_t ts)
    {
        auto it = std::find_if(
            windowReservoirs.begin(),
            windowReservoirs.end(),
            [&](const auto& pair)
            {
                auto key = pair.first;
                return ts >= key && ts < key + windowSizeMSec;
            });
        if (it != windowReservoirs.end())
        {
            return it->first;
        }
        return std::nullopt;
    }
    size_t getCorrespondingWindowReservoirSize(uint64_t ts)
    {
        auto key = findCorrespondingWindow(ts);
        if (key.has_value() && windowReservoirs.contains(key.value()))
        {
            return windowReservoirs.at(key.value()).size();
        }
        return 0;
    }
    uint64_t getWindowStart(uint64_t timestamp, uint64_t previousWindowStart)
    {
        uint64_t windowStart = previousWindowStart;
        uint64_t windowEnd = windowStart + this->windowSizeMSec;
        /// `windowStart` (current window) has overtaken input
        if (timestamp < windowStart)
        {
            /// Go back in the map until we find the window this record belongs to
            auto key = this->findCorrespondingWindow(timestamp);
            if (key.has_value())
            {
                windowStart = key.value();
            }
            else
            {
                NES_ERROR("Failed to find older window.");
                exit(1);
            }
        }
        /// New window necessary
        else if (timestamp >= windowEnd)
        {
            while (timestamp >= windowEnd)
            {
                windowEnd += this->windowSizeMSec;
            }
            windowStart = windowEnd - this->windowSizeMSec;
        }
        /// else we are still in the same window: timestamp >= windowStart && timestamp < upperBound
        return windowStart;
    }
    void addRecordToCorrespondingWindowReservoir(uint64_t windowStart, std::tuple<uint64_t, uint64_t, uint64_t> record)
    {
        this->windowReservoirs.try_emplace(windowStart).first->second.emplaceBack(record);
    }
    void replaceRecordInCorrespondingWindowReservoir(uint64_t windowStart, uint64_t index, std::tuple<uint64_t, uint64_t, uint64_t> record)
    {
        this->windowReservoirs.try_emplace(windowStart).first->second.replace(index, record);
    }
    WindowReservoir& getCorrespondingWindowReservoir(uint64_t windowStart)
    {
        return this->windowReservoirs.at(windowStart);
    }
    std::map<uint64_t, WindowReservoir>& getSampleStore() { return windowReservoirs; }
    uint64_t getReservoirSize() const { return reservoirSize; }
    uint64_t getWindowSizeMSec() const { return windowSizeMSec; }
private:
    std::map<uint64_t, WindowReservoir> windowReservoirs;
    /// Number of tuples in the reservoir
    uint64_t reservoirSize;
    /// Size of the window in milliseconds
    uint64_t windowSizeMSec;
};

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
        NES_ERROR("Failed to convert field id in line {}", line);
        return std::nullopt;
    }
    result = std::from_chars(valueString.data(), valueString.data() + valueString.size(), value);
    if (result.ec == std::errc::invalid_argument || result.ec == std::errc::result_out_of_range)
    {
        NES_ERROR("Failed to convert field value in line {}", line);
        return std::nullopt;
    }
    result = std::from_chars(timestampString.data(), timestampString.data() + timestampString.size(), timestamp);
    if (result.ec == std::errc::invalid_argument || result.ec == std::errc::result_out_of_range)
    {
        NES_ERROR("Failed to convert field timestamp in line {}", line);
        return std::nullopt;
    }

    return std::make_tuple(id, value, timestamp);
}

/// Simulate a reservoir sample over a tumbling window
int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <csv_file_path>\nCSV layout: id,value,timestamp in msec\n";
        return 1;
    }
    std::ifstream file(argv[1]);
    if (!file.is_open())
    {
        std::cerr << "Failed to open file: " << argv[1] << '\n';
        return 1;
    }
    std::string fileContent((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    auto lines = fileContent | std::views::split('\n');
    auto firstLine = *lines.begin();
    /// Check whether there is a header.
    auto filteredLines =(not firstLine.empty() && std::isalpha(*firstLine.begin()))
    ? lines | std::views::drop(1)
    : lines | std::views::drop(0);
    auto parsedRecords = filteredLines
        | std::views::transform([](auto&& line)
        {
            return parseLine(std::string_view(line.begin(), line.end()));
        })
        | std::views::filter([](const auto& result)
        {
            return result.has_value();
        });
    auto sampleStore = ReservoirSampleStore(5, 5000);
    uint64_t windowStart = 0;
    for (const auto& parsedRecord : parsedRecords)
    {
        /// Probably still assumes timestamps are ordered at some point in the code
        auto record = parsedRecord.value();
        auto timestamp = std::get<2>(record);
        windowStart = sampleStore.getWindowStart(timestamp, windowStart);

        /// Reservoir is not yet full
        if (sampleStore.getCorrespondingWindowReservoirSize(windowStart) < sampleStore.getReservoirSize())
        {
            sampleStore.addRecordToCorrespondingWindowReservoir(windowStart, record);
        }
        /// Draw to find out if we need to replace a record in the reservoir
        else
        {
            static std::mt19937 gen(NES::Runtime::Execution::Aggregation::Synopsis::ReservoirSampleFunction::GENERATOR_SEED);
            uint64_t currentIndex = sampleStore.getCorrespondingWindowReservoir(windowStart).getIndex();
            std::uniform_int_distribution<> dis(0, static_cast<int>(currentIndex));
            uint64_t random = dis(gen);
            if (random < sampleStore.getReservoirSize())
            {
                sampleStore.replaceRecordInCorrespondingWindowReservoir(windowStart, random, record);
            }
            sampleStore.getCorrespondingWindowReservoir(windowStart).increaseIndex();
        }
    }

    std::cout << "window start, window end, (reservoir sample)\n";
    for (const auto& [windowStart, windowReservoir] : sampleStore.getSampleStore())
    {
        for (auto record : windowReservoir.getReservoir())
        {
            std::cout << fmt::format(
                "{}, {}, ({}, {}, {})\n",
                windowStart,
                windowStart + sampleStore.getWindowSizeMSec(),
                std::get<0>(record),
                std::get<1>(record),
                std::get<2>(record));
        }
    }
    return 0;
}
