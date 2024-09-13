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

#include <functional>
#include <iostream>
#include <map> ///-Todo: remove after refactoring to unordered_map
#include <optional>
#include <string>
#include <unordered_map>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Sources
{

class RegistrySourceValidation final
{
public:
    RegistrySourceValidation();
    ~RegistrySourceValidation() = default;

    template <bool update = false>
    void registerPlugin(
        const std::string& name, const std::function<SourceDescriptor::Config(std::map<std::string, std::string>&& sourceConfig)>& creator)
    {
        if (!update && registry.contains(name))
        {
            std::cerr << "Plugin '" << name << "', but it already exists.\n";
            return;
        }
        registry[name] = creator;
    }

    std::optional<SourceDescriptor::Config> tryCreate(const std::string& name, std::map<std::string, std::string>&& sourceConfig) const;

    [[nodiscard]] bool contains(const std::string& name) const;

    static RegistrySourceValidation& instance();

private:
    std::unordered_map<std::string, std::function<SourceDescriptor::Config(std::map<std::string, std::string>&& sourceConfig)>> registry;
};

}
