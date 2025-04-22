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
#include <memory>
#include <DataTypes/Schema.hpp>
#include <Util/OperatorsUtil.hpp>

namespace Util
{

std::string detail::concatenateFunctionHelper(uint64_t value)
{
    std::ostringstream ss;
    ss << value;
    return ss.str();
}

std::string detail::concatenateFunctionHelper(const NES::Schema& schema)
{
    std::stringstream ss;
    ss << schema;
    return ss.str();
}

}
