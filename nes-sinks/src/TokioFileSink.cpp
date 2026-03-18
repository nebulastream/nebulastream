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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/TokioSink.hpp>
#include <TokioSinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <BackpressureChannel.hpp>

namespace NES
{

// Declared in TokioSink.cpp
TokioSink::SpawnFn makeFileSinkSpawnFn(std::string filePath);

/// Config parameters for TokioFileSink.
struct ConfigParametersTokioFileSink
{
    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path", "",
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            FILE_PATH);
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
/// NOTE: TokioSinkRegistryArguments does NOT contain BackpressureController (per user decision,
/// the struct has exactly {SinkDescriptor, channelCapacity}). We construct a default
/// BackpressureController{} here when creating TokioSink, since TokioSink manages its own
/// internal backpressure via the Rust channel and does not use the C++ backpressure mechanism.
TokioSinkRegistryReturnType
TokioSinkGeneratedRegistrar::RegisterTokioFileSinkTokioSink(TokioSinkRegistryArguments args)
{
    const auto filePath = args.sinkDescriptor.getFromConfig(ConfigParametersTokioFileSink::FILE_PATH);

    // TokioSink manages its own backpressure via the Rust channel, so it does not use the C++
    // backpressure mechanism. Create a disconnected BackpressureController; the listener is discarded.
    auto [bpc, _listener] = createBackpressureChannel();
    return std::make_unique<TokioSink>(
        std::move(bpc),
        makeFileSinkSpawnFn(filePath),
        args.channelCapacity);
}

} // namespace NES
