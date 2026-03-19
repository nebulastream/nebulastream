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

/// TokioFileSink registration.
///
/// Registers "TokioFileSink" in TokioSinkRegistry and SinkValidationRegistry.
/// No Sink subclass -- the factory creates a TokioSink directly with a SpawnFn
/// that calls spawn_file_sink FFI.

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/TokioSink.hpp>
#include <TokioSinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES
{

// Declared in TokioSink.hpp
// TokioSink::SpawnFn makeFileSinkSpawnFn(std::string filePath, std::vector<SinkSchemaField> schema);

/// Config parameters for TokioFileSink.
struct ConfigParametersTokioFileSink
{
    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path", "",
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    /// Override OUTPUT_FORMAT with a default so it is not mandatory at validation time.
    /// The systest binder injects output_format=CSV after validation for inline sinks.
    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format", "CSV",
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(OUTPUT_FORMAT, config); }};

    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            FILE_PATH, OUTPUT_FORMAT, SinkDescriptor::ADD_TIMESTAMP);
};

} // namespace NES

// --- Registry functions (must be in namespace NES to match generated declarations) ---

namespace NES
{

/// Validation: parse and validate TokioFileSink config params.
SinkValidationRegistryReturnType RegisterTokioFileSinkSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTokioFileSink>(
        std::move(sinkConfig.config), "TokioFileSink");
}

/// Factory: create a TokioSink with a FileSink SpawnFn.
TokioSinkRegistryReturnType
TokioSinkGeneratedRegistrar::RegisterTokioFileSinkTokioSink(TokioSinkRegistryArguments args)
{
    const auto filePath = args.sinkDescriptor.getFromConfig(ConfigParametersTokioFileSink::FILE_PATH);

    // Build structured schema for the Rust sink.
    std::vector<SinkSchemaField> schemaFields;
    if (const auto& schema = args.sinkDescriptor.getSchema())
    {
        for (const auto& field : schema->getFields())
        {
            schemaFields.push_back({
                field.name,
                std::string(magic_enum::enum_name(field.dataType.type)),
                field.dataType.nullable});
        }
    }

    return std::make_unique<TokioSink>(
        std::move(args.backpressureController),
        makeFileSinkSpawnFn(filePath, std::move(schemaFields)),
        args.channelCapacity);
}

} // namespace NES
