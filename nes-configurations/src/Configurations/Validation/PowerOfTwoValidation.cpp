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

#include <bit>
#include <charconv>
#include <cstdint>
#include <string>
#include <system_error>

namespace NES
{

bool PowerOfTwoValidation::isValid(const std::string& parameter) const
{
    uint64_t value = 0;
    const char* begin = parameter.data();
    const char* end = begin + parameter.size();
    /// from_chars does not throw: it rejects non-numeric input, partial parses, and overflow via the error code.
    const auto [ptr, errorCode] = std::from_chars(begin, end, value);
    if (errorCode != std::errc{} || ptr != end)
    {
        return false;
    }
    return value > 0 && std::has_single_bit(value);
}
}
