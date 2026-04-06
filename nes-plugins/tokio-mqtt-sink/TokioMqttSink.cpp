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

/// TokioMqttSink registration.
///
/// Registers "TokioMqttSink" in TokioSinkRegistry and SinkValidationRegistry.
/// No Sink subclass -- the factory creates a TokioSink directly with a SpawnFn
/// that calls spawn_mqtt_sink FFI.

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/TokioSink.hpp>
#include <TokioSinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

/// Config parameters for TokioMqttSink.
struct ConfigParametersTokioMqttSink
{
    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> MQTT_TOPIC{
        "mqtt_topic", "",
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(MQTT_TOPIC, config); }};

    /// Override OUTPUT_FORMAT with a default so it is not mandatory at validation time.
    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format", "CSV",
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(OUTPUT_FORMAT, config); }};

    // NOLINTNEXTLINE(cert-err58-cpp)
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            MQTT_TOPIC, OUTPUT_FORMAT, SinkDescriptor::ADD_TIMESTAMP);
};

} // namespace NES

// --- Registry functions (must be in namespace NES to match generated declarations) ---

namespace NES
{

/// Validation: parse and validate TokioMqttSink config params.
SinkValidationRegistryReturnType RegisterTokioMqttSinkSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTokioMqttSink>(
        std::move(sinkConfig.config), "TokioMqttSink");
}

/// Factory: create a TokioSink with an MqttSink SpawnFn.
TokioSinkRegistryReturnType
TokioSinkGeneratedRegistrar::RegisterTokioMqttSinkTokioSink(TokioSinkRegistryArguments args)
{
    const auto mqttTopic = args.sinkDescriptor.getFromConfig(ConfigParametersTokioMqttSink::MQTT_TOPIC);

    return std::make_unique<TokioSink>(
        std::move(args.backpressureController),
        makeMqttSinkSpawnFn(mqttTopic),
        args.channelCapacity);
}

} // namespace NES
