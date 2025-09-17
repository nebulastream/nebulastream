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

#include <algorithm>
#include <chrono>
#include <iostream>
#include <ratio>
#include <string_view>
#include <thread>
#include <Util/Strings.hpp>
#include <fmt/format.h>

namespace NES
{

/// We can assume that the parsing will work, as the config has been validated
FixedGeneratorRate::FixedGeneratorRate(const std::string_view configString)
    : emitRateTuplesPerSecond(parseAndValidateConfigString(configString).value())
{
}

std::optional<double> FixedGeneratorRate::parseAndValidateConfigString(std::string_view configString)
{
    const auto parameterAndValue = Util::splitWithStringDelimiter<std::string_view>(configString, " ");
    if (parameterAndValue.size() != 2)
    {
        throw InvalidConfigParameter("bad number of arguments for Fixed Generator Rate. Expected `emit_rate <TuplesPerSecond>`");
    }
    if (Util::toLowerCase(parameterAndValue[0]) != "emit_rate")
    {
        throw InvalidConfigParameter(
            "bad argument '{}'. Expected `emit_rate <TuplesPerSecond>`", Util::escapeSpecialCharacters(parameterAndValue[0]));
    }

    auto emitRate = Util::from_chars<double>(parameterAndValue[1]);
    if (!emitRate)
    {
        throw InvalidConfigParameter(
            "bad value '{}' for `emit_rate`. Expected floating point value", Util::escapeSpecialCharacters(parameterAndValue[1]));
    }

    return *emitRate;
}

uint64_t FixedGeneratorRate::calcNumberOfTuplesForInterval(
    const std::chrono::time_point<std::chrono::system_clock>& start, const std::chrono::time_point<std::chrono::system_clock>& end)
{
    /// As the emit rate is fixed, we have to multiply the interval duration (or size) with the emit rate.
    const auto duration = std::chrono::duration<double>(end - start);
    const auto numberOfTuples = emitRateTuplesPerSecond * static_cast<double>(duration.count());
    return numberOfTuples;
}
}
