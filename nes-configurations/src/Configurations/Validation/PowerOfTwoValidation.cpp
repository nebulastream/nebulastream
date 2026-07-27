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

#include <Configurations/Validation/PowerOfTwoValidation.hpp>

#include <cstdint>
#include <regex>
#include <string>

namespace NES
{

bool PowerOfTwoValidation::isValid(const std::string& parameter) const
{
    /// Reject anything that is not a plain integer, and cap the digit count so std::stoul cannot overflow.
    const std::regex numberRegex("^\\d+$");
    if (constexpr auto capDigitCount = 10; !std::regex_match(parameter, numberRegex) || parameter.length() > capDigitCount)
    {
        return false;
    }

    /// A positive power of two has exactly one set bit, so n & (n - 1) clears it to zero.
    const uint64_t value = std::stoul(parameter);
    return value > 0 && (value & (value - 1)) == 0;
}
}
