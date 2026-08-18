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

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Util/Hash.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

bool hasNoOverrides(const std::vector<ConfigurationOverride>& alternatives)
{
    return alternatives.empty() or (alternatives.size() == 1 and alternatives.front().empty());
}

std::vector<ConfigurationOverride> combine(
    const std::vector<ConfigurationOverride>& overrides,
    const std::vector<ConfigurationOverride>& otherOverrides,
    const OnDuplicateKey onDuplicateKey)
{
    if (hasNoOverrides(overrides) and hasNoOverrides(otherOverrides))
    {
        return {ConfigurationOverride{}};
    }
    if (hasNoOverrides(overrides))
    {
        return otherOverrides;
    }
    if (hasNoOverrides(otherOverrides))
    {
        return overrides;
    }

    std::vector<ConfigurationOverride> combined;
    combined.reserve(overrides.size() * otherOverrides.size());
    for (const auto& override : overrides)
    {
        for (const auto& other : otherOverrides)
        {
            auto merged = other;
            for (const auto& [key, value] : override)
            {
                if (onDuplicateKey == OnDuplicateKey::Reject and merged.contains(key))
                {
                    throw SLTUnexpectedToken("Configuration key '{}' is set more than once for the same query", key);
                }
                merged[key] = value;
            }
            /// Two pairings with repeated values merge to the same override, leading to duplicate execution of a query.
            if (not std::ranges::contains(combined, merged))
            {
                combined.push_back(std::move(merged));
            }
        }
    }
    return combined;
}

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
