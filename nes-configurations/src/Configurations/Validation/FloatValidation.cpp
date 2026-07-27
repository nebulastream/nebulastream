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

#include <Configurations/Validation/FloatValidation.hpp>

#include <regex>
#include <string>

namespace NES
{

bool FloatValidation::isValid(const std::string& parameter) const
{
    /// Checking if the parameter can be parsed to a floating point
    const std::regex numberRegex(R"(^\d*\.?\d+$)");
    if (!std::regex_match(parameter, numberRegex))
    {
        return false;
    }

    /// Checking if the values lies between min and max
    const double parsedNumber = std::stod(parameter);
    return parsedNumber >= min && parsedNumber <= max;
}
}
