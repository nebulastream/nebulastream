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

#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>
#include <Util/Strings.hpp>

namespace NES
{

SinusGeneratorRate::SinusGeneratorRate(const double frequency, const double amplitude) : frequency(frequency), amplitude(amplitude)
{
}

std::optional<std::tuple<double, double>> SinusGeneratorRate::parseAndValidateConfigString(std::string_view configString)
{
    std::optional<double> amplitude = {};
    std::optional<double> frequency = {};

    std::vector<std::string_view> params;

    for (const auto& param : splitOnMultipleDelimiters(configString, {'\n', ','}))
    {
        params.emplace_back(trimWhiteSpaces(std::string_view(param)));
    }

    if (params.size() == 2)
    {
        const auto amplitudeParams = splitWithStringDelimiter<std::string_view>(params[0], " ");
        if (toLowerCase(amplitudeParams[0]) == "amplitude")
        {
            amplitude = from_chars<double>(amplitudeParams[1]);
        }

        const auto frequencyParams = splitWithStringDelimiter<std::string_view>(params[1], " ");
        if (toLowerCase(frequencyParams[0]) == "frequency")
        {
            frequency = from_chars<double>(frequencyParams[1]);
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
    const auto numberOfTuples = static_cast<uint64_t>(integralStartEnd(startTimePoint, endTimePoint, amplitude, frequency));
    return numberOfTuples;
}

}
