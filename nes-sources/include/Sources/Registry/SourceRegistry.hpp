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
#include <Sources/Source.hpp>

namespace NES::Sources
{

class SourceRegistry final
{
public:
    virtual ~SourceRegistry() = default;

    template <bool update = false>
    void registerPlugin(const std::string& name, const std::function<std::unique_ptr<Source>()>& creator)
    {
        if (!update && registry.contains(name))
        {
            std::cerr << "Plugin '" << name << "', but it already exists.\n";
            return;
        }
        registry[name] = creator;
    }

    template <typename Derived>
    std::optional<std::unique_ptr<Derived>> tryCreateAs(const std::string& name)
    {
        if (registry.contains(name))
        {
            auto basePointer = registry[name]();
            auto* tmp = dynamic_cast<Derived*>(basePointer.get());
            /// ReSharper disable once CppDFAConstantConditions, incorrect assumption: tmp can never be a nullptr
            if (tmp != nullptr && basePointer.release())
            {
                std::unique_ptr<Derived> derivedPointer;
                derivedPointer.reset(tmp);
                return std::optional(std::move(derivedPointer));
            }
            std::cerr << "Failed to cast from " << typeid(basePointer.get()).name() << " to " << typeid(Derived).name() << ".\n";
            return std::nullopt;
        }
        std::cerr << "Data source " << name << " not found.\n";
        return std::nullopt;
    }

    std::optional<std::unique_ptr<Source>> tryCreate(const std::string& name) const;

    [[nodiscard]] bool contains(const std::string& name) const;

    static SourceRegistry& instance();

private:
    SourceRegistry();
    std::unordered_map<std::string, std::function<std::unique_ptr<Source>()>> registry;
};

}
