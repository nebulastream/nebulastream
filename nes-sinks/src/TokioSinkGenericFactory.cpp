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

/// Generic factory for Rust-backed Tokio sinks.
///
/// Provides makeNamedSinkSpawnFn() which creates a SpawnFn that calls the generic
/// spawn_sink_by_name() FFI entry point. Also provides the RUST_TOKIO_SINK_IMPL
/// macro which generates TokioSink factory functions for each registered Rust sink.
///
/// When adding a new Rust sink, add a RUST_TOKIO_SINK_IMPL(Name) invocation below.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/TokioSink.hpp>
#include <TokioSinkImpl.hpp>
#include <TokioSinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum/magic_enum.hpp>

// CXX-generated header for the ffi_sink module
#include <nes-sink-bindings/lib.h>

namespace NES
{

/// Helper for std::visit with multiple lambdas.
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

/// Convert DescriptorConfig::Config (typed variant map) to parallel std::vector<std::string>.
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

/// Serialize a Schema into the __schema_fields config format:
/// "name:TYPE:NOT_NULLABLE,name:TYPE:IS_NULLABLE,..."
static std::string serializeSchemaToConfig(const Schema& schema)
{
    std::string result;
    for (size_t i = 0; i < schema.getFields().size(); ++i)
    {
        if (i > 0) result += ',';
        const auto& field = schema.getFields()[i];
        result += field.name;
        result += ':';
        result += magic_enum::enum_name(field.dataType.type);
        result += ':';
        result += field.dataType.nullable ? "IS_NULLABLE" : "NOT_NULLABLE";
    }
    return result;
}

/// Create a SpawnFn that calls the generic spawn_sink_by_name() FFI with the given sink name and config.
TokioSink::SpawnFn makeNamedSinkSpawnFn(std::string sinkName, DescriptorConfig::Config config)
{
    return [name = std::move(sinkName), cfg = std::move(config)](
               uint64_t sinkId, uint32_t channelCapacity,
               uintptr_t errorFnPtr, uintptr_t errorCtxPtr) -> std::unique_ptr<TokioSink::RustSinkHandleImpl>
    {
        auto [keys, values] = configToStringVectors(cfg);
        auto handle = ::spawn_sink_by_name(
            std::string(name), sinkId, keys, values,
            channelCapacity, errorFnPtr, errorCtxPtr);
        return std::make_unique<TokioSink::RustSinkHandleImpl>(std::move(handle));
    };
}

/// Convert raw string map to parallel std::vector<std::string>.
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

/// Reconstruct typed DescriptorConfig::Config from Rust validation output.
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

std::vector<std::string> NES::getRegisteredTokioSinkNames()
{
    std::vector<std::string> result;
    for (const auto& name : ::get_registered_sink_names())
    {
        result.emplace_back(std::string(name));
    }
    return result;
}

NES::TokioSinkRegistryReturnType NES::createTokioSinkFromRegistry(
    std::string name, NES::TokioSinkRegistryArguments args)
{
    auto config = args.sinkDescriptor.getConfig();
    if (const auto& schema = args.sinkDescriptor.getSchema())
    {
        config["__schema_fields"] = NES::serializeSchemaToConfig(*schema);
    }
    return std::make_unique<NES::TokioSink>(
        std::move(args.backpressureController),
        NES::makeNamedSinkSpawnFn(std::move(name), std::move(config)),
        args.channelCapacity);
}

NES::SinkValidationRegistryReturnType NES::validateTokioSinkFromRegistry(
    std::string name, NES::SinkValidationRegistryArguments sinkConfig)
{
    NES::DescriptorConfig::Config baseParams;
    for (const auto& [key, container] : NES::SinkDescriptor::parameterMap)
    {
        if (sinkConfig.config.contains(key))
        {
            if (auto val = container.validate(sinkConfig.config); val.has_value())
                baseParams.emplace(key, val.value());
            sinkConfig.config.erase(key);
        }
        else if (auto def = container.getDefaultValue(); def.has_value())
        {
            baseParams.emplace(key, def.value());
        }
    }
    auto [keys, values] = NES::stringMapToStringVectors(sinkConfig.config);
    rust::Vec<rust::String> outKeys, outValues, outTypes;
    try {
        ::validate_sink_config(std::string(name), keys, values, outKeys, outValues, outTypes);
    } catch (const std::exception& e) {
        throw NES::InvalidConfigParameter("Invalid {} sink configuration: {}", name, e.what());
    }
    auto result = NES::reconstructTypedConfig(outKeys, outValues, outTypes);
    result.merge(baseParams);
    return result;
}
