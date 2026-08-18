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

#include <Model/ConfigurationOverride.hpp>

#include <cstddef>
#include <string>
#include <string_view>

#include <Util/Hash.hpp>

namespace NES
{

std::string& ConfigurationOverride::operator[](const std::string_view key)
{
    return overrideParameters[std::string{key}];
}

const std::string& ConfigurationOverride::at(const std::string_view key) const
{
    return overrideParameters.at(std::string{key});
}

bool ConfigurationOverride::contains(const std::string_view key) const
{
    return overrideParameters.contains(std::string{key});
}

bool ConfigurationOverride::empty() const
{
    return overrideParameters.empty();
}

std::size_t ConfigurationOverride::size() const
{
    return overrideParameters.size();
}

}

std::size_t std::hash<NES::ConfigurationOverride>::operator()(const NES::ConfigurationOverride& configurationOverride) const noexcept
{
    return NES::Hash{}(configurationOverride.overrideParameters);
}
