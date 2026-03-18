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
/// When adding a new Rust source, add a RUST_TOKIO_SOURCE_IMPL(Name) invocation
/// in both macro sections below.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <ErrorHandling.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/TokioSource.hpp>
#include <TokioSourceImpl.hpp>
#include <TokioSourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

// CXX-generated header for the ffi_source module
#include <nes-source-bindings/lib.h>

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

// --- Functions called by the registry templates ---

std::vector<std::string> NES::getRegisteredTokioSourceNames()
{
    std::vector<std::string> result;
    for (const auto& name : ::get_registered_source_names())
    {
        result.emplace_back(std::string(name));
    }
    return result;
}

NES::TokioSourceRegistryReturnType NES::createTokioSourceFromRegistry(
    std::string name, NES::TokioSourceRegistryArguments args)
{
    return std::make_unique<NES::TokioSource>(
        args.originId, args.inflightLimit, std::move(args.bufferProvider),
        NES::makeNamedSpawnFn(std::move(name), args.sourceDescriptor.getConfig()));
}

NES::SourceValidationRegistryReturnType NES::validateTokioSourceFromRegistry(
    std::string name, NES::SourceValidationRegistryArguments sourceConfig)
{
    NES::DescriptorConfig::Config baseParams;
    for (const auto& [key, container] : NES::SourceDescriptor::parameterMap)
    {
        if (sourceConfig.config.contains(key))
        {
            if (auto val = container.validate(sourceConfig.config); val.has_value())
                baseParams.emplace(key, val.value());
            sourceConfig.config.erase(key);
        }
        else if (auto def = container.getDefaultValue(); def.has_value())
        {
            baseParams.emplace(key, def.value());
        }
    }
    auto [keys, values] = NES::stringMapToStringVectors(sourceConfig.config);
    rust::Vec<rust::String> outKeys, outValues, outTypes;
    try {
        ::validate_source_config(std::string(name), keys, values, outKeys, outValues, outTypes);
    } catch (const std::exception& e) {
        throw NES::InvalidConfigParameter("Invalid {} source configuration: {}", name, e.what());
    }
    auto result = NES::reconstructTypedConfig(outKeys, outValues, outTypes);
    result.merge(baseParams);
    return result;
}
