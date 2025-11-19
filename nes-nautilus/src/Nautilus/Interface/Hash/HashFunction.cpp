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

#include <Nautilus/Interface/Hash/HashFunction.hpp>

#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <static.hpp>

namespace NES
{
HashFunction::HashValue HashFunction::calculate(const VarVal& value) const
{
    auto hash = init();
    return calculate(hash, value);
};

HashFunction::HashValue HashFunction::calculate(const std::vector<VarVal>& values) const
{
    auto hash = init();
    for (const auto& value : nautilus::static_iterable(values))
    {
        hash = calculate(hash, value);
    }
    return hash;
}
}
