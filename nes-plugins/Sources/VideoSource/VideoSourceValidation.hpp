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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <sys/socket.h> /// For socket functions
#include <sys/types.h>

namespace NES
{

struct ConfigParametersVideo
{
    static inline const DescriptorConfig::ConfigParameter<uint32_t> SOURCE_SELECTOR{
        "sourceSelector",
        0,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto source_selector = Util::from_chars<uint32_t>(config.at(SOURCE_SELECTOR));
            if (!source_selector)
            {
                NES_ERROR("Source selector is not a valid positive integer");
                return std::nullopt;
            }
            if (source_selector.value() > 3)
            {
                NES_ERROR("VideoSource Source selector {} is invalid (valid options are 0 for RGB and 1 for IR)", source_selector.value());
                return std::nullopt;
            }
            return source_selector;
        }};
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SOURCE_SELECTOR);
};

struct ConfigParametersVideoPlayback
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "filePath",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            const auto path = DescriptorConfig::tryGet(FILEPATH, config);
            return path;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(FILEPATH);
};

struct ConfigParametersMQTTVideoPlayback
{
    static inline const DescriptorConfig::ConfigParameter<std::string> SERVER_URI{
        "serverURI",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SERVER_URI, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CLIENT_ID{
        "clientId",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CLIENT_ID, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "topic",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TOPIC, config); }};

    static inline const DescriptorConfig::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flushIntervalMS",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};

    static inline const DescriptorConfig::ConfigParameter<int32_t> QOS{
        "qos",
        1,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint8_t>
        {
            int32_t qos = std::stoi(config.at(QOS));
            if (qos != 0 && qos != 1 && qos != 2)
            {
                NES_ERROR("MQTTSource: QualityOfService is: {}, but must be 0, 1, or 2.", qos);
                return std::nullopt;
            }
            return qos;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SERVER_URI, CLIENT_ID, QOS, TOPIC, FLUSH_INTERVAL_MS);
};

}
