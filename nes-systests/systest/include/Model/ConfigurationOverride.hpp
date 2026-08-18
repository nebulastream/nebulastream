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
#include <string>
#include <string_view>
#include <unordered_map>

namespace NES
{

/// The worker settings that one configuration block of a test file overrides, as raw key value pairs.
/// Lookup, iteration, and equality behave as on the `std::unordered_map` that holds the pairs.
class ConfigurationOverride
{
public:
    std::string& operator[](std::string_view key);
    [[nodiscard]] const std::string& at(std::string_view key) const;
    [[nodiscard]] bool contains(std::string_view key) const;
    [[nodiscard]] bool empty() const;
    [[nodiscard]] std::size_t size() const;

    [[nodiscard]] auto begin() const { return overrideParameters.begin(); }

    [[nodiscard]] auto end() const { return overrideParameters.end(); }

    bool operator==(const ConfigurationOverride& other) const = default;

private:
    friend struct std::hash<ConfigurationOverride>;

    std::unordered_map<std::string, std::string> overrideParameters;
};

}

template <>
struct std::hash<NES::ConfigurationOverride>
{
    std::size_t operator()(const NES::ConfigurationOverride& configurationOverride) const noexcept;
};
