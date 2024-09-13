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
#include <optional>
#include <string>
#include <unordered_map>
#include <API/Schema.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Sources
{

class RegistrySource final
{
public:
    RegistrySource();
    ~RegistrySource() = default;

    template <bool update = false>
    void registerPlugin(
        const std::string& name,
        const std::function<std::unique_ptr<Source>(const Schema& schema, const SourceDescriptor& sourceDescriptor)>& creator)
    {
        if (!update && registry.contains(name))
        {
            std::cerr << "Plugin '" << name << "', but it already exists.\n";
            return;
        }
        registry[name] = creator;
    }

    std::optional<std::unique_ptr<Source>>
    tryCreate(const std::string& name, const Schema& schema, const SourceDescriptor& sourceDescriptor) const;

    [[nodiscard]] bool contains(const std::string& name) const;

    static RegistrySource& instance();

private:
    std::unordered_map<std::string, std::function<std::unique_ptr<Source>(const Schema& schema, const SourceDescriptor& sourceDescriptor)>>
        registry;
};

}
