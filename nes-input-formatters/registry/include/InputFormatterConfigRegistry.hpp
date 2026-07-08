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
#include <utility>

#include <Configurations/ConfigValue.hpp>
#include <Util/Reflection.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// Bridges the generic, frontend-facing InstantiatedConfig and the arbitrary config struct an
/// input formatter defines for itself (e.g. CSVInputFormatterConfig). The struct travels through
/// the InputFormatterDescriptor type-erased as std::any; reflect/unreflect keep the descriptor
/// serializable — only the one struct per formatter needs to be reflectable, and only this entry
/// knows its concrete type.
struct InputFormatterConfigEntry
{
    /// Build the formatter-defined config struct from a resolved generic config.
    std::function<std::any(const InstantiatedConfig&)> instantiate;
    /// Serialize the type-erased config struct (safe to any_cast: only instantiate/unreflect produce it).
    std::function<Reflected(const std::any&)> reflect;
    /// Deserialize back into the type-erased config struct.
    std::function<std::any(const Reflected&, const ReflectionContext&)> unreflect;
};

class InputFormatterConfigRegistry
    : public RuntimeRegistry<InputFormatterConfigRegistry, std::string, InputFormatterConfigEntry, /*CaseSensitive*/ false>
{
};

/// ConfigStruct must provide `static ConfigStruct fromConfig(const InstantiatedConfig&)` and be
/// reflectable/unreflectable. Used by the generated registration glue
/// (see create_runtime_registry(InputFormatterConfig ...) in nes-input-formatters/CMakeLists.txt).
template <typename ConfigStruct>
InputFormatterConfigEntry makeInputFormatterConfigEntry()
{
    return InputFormatterConfigEntry{
        .instantiate = [](const InstantiatedConfig& config) { return std::any{ConfigStruct::fromConfig(config)}; },
        .reflect = [](const std::any& config) { return reflect(std::any_cast<const ConfigStruct&>(config)); },
        .unreflect = [](const Reflected& data, const ReflectionContext& context) { return std::any{context.unreflect<ConfigStruct>(data)}; },
    };
}

}
