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

/// Generic factory for Rust-backed Tokio sources.
///
/// Provides makeNamedSpawnFn() which creates a SpawnFn that calls the generic
/// spawn_source_by_name() FFI entry point. Also provides the RUST_TOKIO_SOURCE_IMPL
/// macro which generates both TokioSource factory and SourceValidation functions
/// for each registered Rust source.
///
/// The RustTokioSources.inc file is CMake-generated and contains one
/// RUST_TOKIO_SOURCE_IMPL(PluginName) invocation per registered source.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/TokioSource.hpp>
#include <TokioSourceImpl.hpp>
#include <TokioSourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

// CXX-generated header for the ffi_source module
#include <nes-source-lib/src/lib.h>

namespace NES
{

/// Helper for std::visit with multiple lambdas.
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

/// Convert DescriptorConfig::Config (typed variant map) to parallel std::vector<std::string>.
/// The CXX bridge declares &CxxVector<CxxString> which maps to const std::vector<std::string>&.
static std::pair<std::vector<std::string>, std::vector<std::string>>
configToStringVectors(const DescriptorConfig::Config& config)
{
    std::vector<std::string> keys, values;
    for (const auto& [key, value] : config)
    {
        keys.push_back(key);
        std::visit(overloaded{
            [&values](const std::string& v) { values.push_back(v); },
            [&values](bool v) { values.push_back(v ? "true" : "false"); },
            [&values](char v) { values.push_back(std::string(1, v)); },
            [&values](const EnumWrapper& v) { values.push_back(v.getValue()); },
            [&values](int32_t v) { values.push_back(std::to_string(v)); },
            [&values](uint32_t v) { values.push_back(std::to_string(v)); },
            [&values](int64_t v) { values.push_back(std::to_string(v)); },
            [&values](uint64_t v) { values.push_back(std::to_string(v)); },
            [&values](float v) { values.push_back(std::to_string(v)); },
            [&values](double v) { values.push_back(std::to_string(v)); },
        }, value);
    }
    return {std::move(keys), std::move(values)};
}

/// Convert raw string map (from validation) to parallel std::vector<std::string>.
static std::pair<std::vector<std::string>, std::vector<std::string>>
stringMapToStringVectors(const std::unordered_map<std::string, std::string>& config)
{
    std::vector<std::string> keys, values;
    for (const auto& [k, v] : config)
    {
        keys.push_back(k);
        values.push_back(v);
    }
    return {std::move(keys), std::move(values)};
}

/// Create a SpawnFn that calls the generic spawn_source_by_name() FFI with the given source name and config.
TokioSource::SpawnFn makeNamedSpawnFn(std::string sourceName, DescriptorConfig::Config config)
{
    return [name = std::move(sourceName), cfg = std::move(config)](
               uint64_t sourceId, uintptr_t bpPtr, uint32_t inflightLimit,
               uintptr_t emitFnPtr, uintptr_t emitCtxPtr,
               uintptr_t errorFnPtr, uintptr_t errorCtxPtr) -> std::unique_ptr<TokioSource::RustHandleImpl>
    {
        auto [keys, values] = configToStringVectors(cfg);
        auto handle = ::spawn_source_by_name(
            std::string(name), sourceId, keys, values,
            bpPtr, inflightLimit, emitFnPtr, emitCtxPtr, errorFnPtr, errorCtxPtr);
        return std::make_unique<TokioSource::RustHandleImpl>(std::move(handle));
    };
}

/// Reconstruct typed DescriptorConfig::Config from validation output.
///
/// The Rust validation returns parallel rust::Vec<rust::String> of (key, value, type_name).
/// This converts them back to the C++ variant map expected by the system.
static DescriptorConfig::Config reconstructTypedConfig(
    const rust::Vec<rust::String>& outKeys,
    const rust::Vec<rust::String>& outValues,
    const rust::Vec<rust::String>& outTypes)
{
    DescriptorConfig::Config result;
    for (size_t i = 0; i < outKeys.size(); i++)
    {
        std::string key(outKeys[i]);
        std::string value(outValues[i]);
        std::string type(outTypes[i]);

        if (type == "u64")
            result.emplace(key, static_cast<uint64_t>(std::stoull(value)));
        else if (type == "i64")
            result.emplace(key, static_cast<int64_t>(std::stoll(value)));
        else if (type == "u32")
            result.emplace(key, static_cast<uint32_t>(std::stoul(value)));
        else if (type == "i32")
            result.emplace(key, static_cast<int32_t>(std::stoi(value)));
        else if (type == "f64")
            result.emplace(key, std::stod(value));
        else if (type == "bool")
            result.emplace(key, value == "true" || value == "1");
        else
            result.emplace(key, value);
    }
    return result;
}

} // namespace NES

// --- CMake-generated macro expansions for TokioSource factories ---

#define RUST_TOKIO_SOURCE_IMPL(PluginName) \
NES::TokioSourceRegistryReturnType \
NES::TokioSourceGeneratedRegistrar::Register##PluginName##TokioSource(NES::TokioSourceRegistryArguments args) \
{ \
    return std::make_unique<NES::TokioSource>( \
        args.originId, args.inflightLimit, std::move(args.bufferProvider), \
        NES::makeNamedSpawnFn(#PluginName, args.sourceDescriptor.getConfig())); \
}

#include "RustTokioSources.inc"
#undef RUST_TOKIO_SOURCE_IMPL

// --- CMake-generated macro expansions for SourceValidation ---

#define RUST_TOKIO_SOURCE_IMPL(PluginName) \
NES::SourceValidationRegistryReturnType NES::Register##PluginName##SourceValidation( \
    NES::SourceValidationRegistryArguments sourceConfig) \
{ \
    /* Extract and validate standard SourceDescriptor params (e.g. max_inflight_buffers) */ \
    /* before passing the remaining config to Rust validation. */ \
    NES::DescriptorConfig::Config baseParams; \
    for (const auto& [key, container] : NES::SourceDescriptor::parameterMap) \
    { \
        if (sourceConfig.config.contains(key)) \
        { \
            if (auto val = container.validate(sourceConfig.config); val.has_value()) \
                baseParams.emplace(key, val.value()); \
            sourceConfig.config.erase(key); \
        } \
        else if (auto def = container.getDefaultValue(); def.has_value()) \
        { \
            baseParams.emplace(key, def.value()); \
        } \
    } \
    /* Validate source-specific params via Rust registry. */ \
    auto [keys, values] = NES::stringMapToStringVectors(sourceConfig.config); \
    rust::Vec<rust::String> outKeys, outValues, outTypes; \
    ::validate_source_config(std::string(#PluginName), keys, values, outKeys, outValues, outTypes); \
    auto result = NES::reconstructTypedConfig(outKeys, outValues, outTypes); \
    /* Merge standard params into result. */ \
    result.merge(baseParams); \
    return result; \
}

#include "RustTokioSources.inc"
#undef RUST_TOKIO_SOURCE_IMPL
