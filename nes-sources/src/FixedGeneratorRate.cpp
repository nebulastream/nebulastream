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
#include <FixedGeneratorRate.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>
#include <Util/Strings.hpp>

namespace NES
{

/// We can assume that the parsing will work, as the config has been validated
FixedGeneratorRate::FixedGeneratorRate(const std::string_view configString)
    : emitRateTuplesPerSecond(parseAndValidateConfigString(configString).value())
{
}

std::optional<double> FixedGeneratorRate::parseAndValidateConfigString(std::string_view configString)
{
    if (const auto params = splitWithStringDelimiter<std::string_view>(configString, " "); params.size() == 2)
    {
        if (toLowerCase(params[0]) == "emit_rate")
        {
            return from_chars<double>(params[1]);
        }
    }
    return {};
}

uint64_t FixedGeneratorRate::calcNumberOfTuplesForInterval(
    const std::chrono::time_point<std::chrono::system_clock>& start, const std::chrono::time_point<std::chrono::system_clock>& end)
{
    /// As the emit rate is fixed, we have to multiply the interval duration (or size) with the emit rate.
    const auto duration = std::chrono::duration<double>(end - start);
    const auto numberOfTuples = static_cast<uint64_t>(emitRateTuplesPerSecond * static_cast<double>(duration.count()));
    return numberOfTuples;
}
}
