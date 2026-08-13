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

struct InputFormatterConfigEntry
{
    std::function<std::expected<ExplicitAny, Exception>(const InstantiatedConfig&)> instantiate;
    std::function<Reflected(const ExplicitAny&, const ReflectionContext&)> reflect;
    std::function<ExplicitAny(const Reflected&, const ReflectionContext&)> unreflect;
};

class InputFormatterConfigRegistry
    : public RuntimeRegistry<InputFormatterConfigRegistry, std::string, InputFormatterConfigEntry, /*CaseSensitive*/ false>
{
public:
    static InputFormatterConfigRegistry& instance();
};

/// ConfigStruct must provide `static ConfigStruct fromConfig(const InstantiatedConfig&)` and be
/// reflectable/unreflectable.
template <typename ConfigStruct>
InputFormatterConfigEntry makeInputFormatterConfigEntry()
{
    return InputFormatterConfigEntry{
        .instantiate =
            [](const InstantiatedConfig& config)
        {
            return ConfigStruct::fromConfig(config).transform([](const ConfigStruct& instance) { return ExplicitAny{std::any{instance}}; });
        },
        .reflect
        = [](const ExplicitAny& config, const ReflectionContext& context) { return context.reflect(config.getAs<const ConfigStruct&>()); },
        .unreflect = [](const Reflected& data, const ReflectionContext& context)
        { return ExplicitAny{std::any{context.unreflect<ConfigStruct>(data)}}; },
    };
}

}
