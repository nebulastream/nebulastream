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

#include <cstddef>
#include <functional>
#include <initializer_list>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace NES
{

/// The worker settings one configuration block of a test file asks for, as raw key value pairs.
struct ConfigurationOverride
{
    std::unordered_map<std::string, std::string> overrideParameters;
    ConfigurationOverride() = default;

    ConfigurationOverride(std::initializer_list<std::pair<std::string_view, std::string_view>> init)
    {
        for (const auto& [key, value] : init)
        {
            overrideParameters.emplace(std::string{key}, std::string{value});
        }
    }

    std::string& operator[](std::string_view key) { return overrideParameters[std::string{key}]; }

    [[nodiscard]] const std::string& at(std::string_view key) const { return overrideParameters.at(std::string{key}); }

    bool operator==(const ConfigurationOverride& other) const = default;
    bool operator!=(const ConfigurationOverride& other) const = default;
};

}

namespace std
{
template <>
struct hash<NES::ConfigurationOverride>
{
    std::size_t operator()(const NES::ConfigurationOverride& co) const noexcept
    {
        std::size_t seed = 0;
        std::hash<std::string> hasher;
        const auto mix = [](std::size_t currentSeed, std::size_t value) noexcept
        { return currentSeed ^ (value + 0x9e3779b9 + (currentSeed << 6U) + (currentSeed >> 2U)); };

        for (const auto& [key, value] : co.overrideParameters)
        {
            seed = mix(seed, hasher(key));
            seed = mix(seed, hasher(value));
        }
        return seed;
    }
};
}
