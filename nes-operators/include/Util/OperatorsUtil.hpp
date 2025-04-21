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

#pragma once

#include <memory>
#include <sstream>
#include <vector>
#include <API/Schema.hpp>

namespace Util
{

namespace detail
{
/**
* @brief set of helper functions for returning the string representation of different types
* @return string representation for a given type
*/
[[maybe_unused]] std::string concatenateFunctionHelper(uint64_t value);
[[maybe_unused]] std::string concatenateFunctionHelper(const NES::Schema& schema);
}

/**
 * @brief concatenates each element of the vector with a comma as the delimiter
 * @tparam T
 * @param vector the vector
 * @return the string representation of the vector
 */
template <typename T>
std::string concatenateVectorAsString(const std::vector<T>& vector)
{
    std::stringstream out;
    for (const auto& item : vector)
    {
        if (item != vector[0])
        {
            out << ", ";
        }
        out << detail::concatenateFunctionHelper(item);
    }
    out << std::endl;
    return out.str();
}

}; /// namespace Util
