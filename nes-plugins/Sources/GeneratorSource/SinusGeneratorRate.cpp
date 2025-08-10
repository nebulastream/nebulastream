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
#include <SinusGeneratorRate.hpp>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

SinusGeneratorRate::SinusGeneratorRate(const double frequency, const double amplitude) : frequency(frequency), amplitude(amplitude)
{
}

std::optional<std::tuple<double, double>> SinusGeneratorRate::parseAndValidateConfigString(std::string_view configString)
{
    std::optional<double> amplitude = {};
    std::optional<double> frequency = {};
    for (const auto& line : configString | std::views::split('\n'))
    {
        auto lineView{std::string_view(line.begin(), line.end())};
        const auto spacePos = lineView.find(' ');
        if (spacePos != std::string_view::npos)
        {
            auto key = lineView.substr(0, spacePos);
            auto value = lineView.substr(spacePos + 1);

            std::string keyLower;
            std::ranges::transform(key, std::back_inserter(keyLower), [](const unsigned char c) { return std::tolower(c); });

            if (keyLower == "amplitude")
            {
                amplitude = std::stod(std::string(value));
            }
            else if (keyLower == "frequency")
            {
                frequency = std::stod(std::string(value));
            }
        }
    }


    if (amplitude.has_value() and frequency.has_value())
    {
        return std::make_tuple(amplitude.value(), frequency.value());
    }
    return {};
}

uint64_t SinusGeneratorRate::calcNumberOfTuplesForInterval(
    const std::chrono::time_point<std::chrono::system_clock>& start, const std::chrono::time_point<std::chrono::system_clock>& end)
{
    /// To calculate the number of tuples for a non-negative sinus, we take the integral of the sinus from end to start
    /// As the sinus must be non-negative, the sin-function is emit_rate_at_x = amplitude * sin(freq * (x - phaseShift)) + amplitude
    /// Thus, the integral from startTimePoint to endTimePoint is the following:
    auto integralStartEnd = [](const double startTimePoint, const double endTimePoint, const double amplitude, const double frequency)
    {
        const auto firstCos = std::cos(frequency * startTimePoint);
        const auto secondCos = frequency * (startTimePoint - endTimePoint);
        const auto thirdCos = std::cos(endTimePoint * frequency);
        return (amplitude * (firstCos - secondCos - thirdCos)) / (2 * frequency);
    };

    /// Calculating the integral of end --> start, resulting in the required number of tuples
    /// As the interval of start and end might share the same seconds, we need to first cast it to milliseconds and then convert it to a
    /// double. Otherwise, we would have the exact same value for startTimePoint and endTimePoint, resulting in number of tuples to generate.
    const auto startTimePoint = std::chrono::duration<double>(start.time_since_epoch()).count();
    const auto endTimePoint = std::chrono::duration<double>(end.time_since_epoch()).count();
    const auto numberOfTuples = integralStartEnd(startTimePoint, endTimePoint, amplitude, frequency);
    return numberOfTuples;
}

}
