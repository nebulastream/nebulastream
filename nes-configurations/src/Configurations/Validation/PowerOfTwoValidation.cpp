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
#include <cstdint>
#include <string>
#include <Util/Strings.hpp>

namespace NES
{

bool PowerOfTwoValidation::isValid(const std::string& parameter) const
{
    const auto value = from_chars<uint64_t>(parameter);
    return value.has_value() && std::has_single_bit(*value);
}
}
