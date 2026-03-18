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

/// TokioGenerator source registration.
///
/// Registers "TokioGenerator" in the TokioSourceRegistry and SourceValidationRegistry.
/// No Source subclass — the factory creates a TokioSource directly with a SpawnFn
/// that calls spawn_generator_source FFI.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/TokioSource.hpp>
#include <TokioSourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

// Declared in TokioSource.cpp
TokioSource::SpawnFn makeGeneratorSpawnFn(uint64_t count, uint64_t intervalMs);

/// Config parameters for TokioGenerator source, used in systests via:
///   CREATE PHYSICAL SOURCE FOR stream TYPE TokioGenerator SET(
///       10 AS `SOURCE`.COUNT,
///       0 AS `SOURCE`.INTERVAL_MS
///   );
struct ConfigParametersTokioGenerator
{
    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<uint64_t> GENERATOR_COUNT{
        "generator_count", 10,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(GENERATOR_COUNT, config); }};

    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<uint64_t> GENERATOR_INTERVAL_MS{
        "generator_interval_ms", 0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(GENERATOR_INTERVAL_MS, config); }};

    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap,
            GENERATOR_COUNT, GENERATOR_INTERVAL_MS);
};

} // namespace NES

// --- Registry functions (must be in namespace NES to match generated declarations) ---

namespace NES
{

/// Validation: parse and validate TokioGenerator config params.
SourceValidationRegistryReturnType RegisterTokioGeneratorSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTokioGenerator>(
        std::move(sourceConfig.config), "TokioGenerator");
}

/// Factory: create a TokioSource with a GeneratorSource SpawnFn.
TokioSourceRegistryReturnType
TokioSourceGeneratedRegistrar::RegisterTokioGeneratorTokioSource(TokioSourceRegistryArguments args)
{
    const auto count = args.sourceDescriptor.getFromConfig(ConfigParametersTokioGenerator::GENERATOR_COUNT);
    const auto intervalMs = args.sourceDescriptor.getFromConfig(ConfigParametersTokioGenerator::GENERATOR_INTERVAL_MS);

    return std::make_unique<TokioSource>(
        args.originId,
        args.inflightLimit,
        std::move(args.bufferProvider),
        makeGeneratorSpawnFn(count, intervalMs));
}

} // namespace NES
