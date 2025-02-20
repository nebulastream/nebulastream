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

#include <regex>
#include <Configurations/Validation/FloatRangeValidation.hpp>

namespace NES::Configurations {
FloatRangeValidation::FloatRangeValidation(const double min, const double max) : min(min), max(max)
{
}

bool FloatRangeValidation::isValid(const std::string& number) const
{
    std::regex numberRegex(R"(^\d*\.?\d+$)");
    if (!std::regex_match(number, numberRegex))
    {
        return false;
    }
    const auto value = std::stod(number);
    return min <= value && value <= max;
}
}
