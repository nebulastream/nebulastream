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

#include <any>
#include <functional>
#include <string>

#include <Configurations/InstantiatedConfigValue.hpp>
#include <Util/Any.hpp>
#include <Util/Reflection.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct SourceConfigEntry
{
    /// Instantiate the config type from the instantiatedConfig
    std::function<std::expected<ExplicitAny, Exception>(const InstantiatedConfig&)> instantiate;
    /// Reflect/Unreflect the type erased config type
    std::function<Reflected(const ExplicitAny&, const ReflectionContext&)> reflect;
    std::function<ExplicitAny(const Reflected&, const ReflectionContext&)> unreflect;
};

class SourceConfigRegistry : public RuntimeRegistry<SourceConfigRegistry, std::string, SourceConfigEntry, /*CaseSensitive*/ false>
{
public:
    static SourceConfigRegistry& instance();
};

/// ConfigStruct must provide `static ConfigStruct fromConfig(const InstantiatedConfig&)` and be
/// reflectable/unreflectable.
template <typename ConfigStruct>
SourceConfigEntry makeSourceConfigEntry()
{
    return SourceConfigEntry{
        .instantiate =
            [](const InstantiatedConfig& config)
        {
            return ConfigStruct::fromConfig(config).transform([](const ConfigStruct& instance) { return ExplicitAny{std::any{instance}}; });
        },
        .reflect
        = [](const ExplicitAny& config, const ReflectionContext& context) { return context.reflect(config.getAs<ConfigStruct>()); },
        .unreflect = [](const Reflected& data, const ReflectionContext& context)
        { return ExplicitAny{std::any{context.unreflect<ConfigStruct>(data)}}; },
    };
}

}
