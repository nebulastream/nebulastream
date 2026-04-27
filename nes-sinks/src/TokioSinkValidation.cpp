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

// Validation-only TU for tokio-backed rust sink plugins. Routes config
// validation through the nes-sink-validation-bindings FFI (pure validation —
// no runtime sink creation) and owns the RegisterTokioSinkValidation symbol
// consumed by nes-sinks-validation's generated registrar. The tokio runtime
// sink class lives in TokioSink.cpp inside nes-sinks (runtime).

#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <rust/cxx.h>

#include <nes-sink-validation-bindings/lib.h>
#include <rfl/json/Parser.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>

#include <Configurations/Descriptor.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

SinkValidationRegistryReturnType RegisterTokioSinkValidation(SinkValidationRegistryArguments args)
{
    std::unordered_map<std::string, std::string> flat;
    for (auto& [key, value] : args.config)
    {
        flat[key.getString()] = std::move(value);
    }
    auto result = validate(args.sinkType, rfl::json::write(flat));
    auto validatedConfig
        = rfl::json::read<std::unordered_map<std::string, std::string>>(std::string_view(result.begin(), result.end())).value();

    DescriptorConfig::Config config;
    for (auto& [key, value] : validatedConfig)
    {
        config.emplace(UppercaseString(std::move(key)), DescriptorConfig::ConfigType{std::move(value)});
    }
    return config;
}

}
