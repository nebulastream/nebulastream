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
#include <Configurations/Validation/BooleanValidation.hpp>

namespace NES::Configurations
{

bool BooleanValidation::isValid(const std::string& parameter) const
{
    std::string lowerParam = parameter;
    std::ranges::transform(lowerParam, lowerParam.begin(), [](unsigned char c) { return std::tolower(c); });

    return lowerParam == "true" || lowerParam == "false" || lowerParam == "1" || lowerParam == "0";
}
}
