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

#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Validation/EndpointValidation.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>

namespace NES
{

/// NOLINTBEGIN(cert-err58-cpp)
struct ConfigParametersNetworkSource
{
    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "channel",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            auto value = DescriptorConfig::tryGet(CHANNEL, config);
            if (value && !stringToUUID(*value))
            {
                NES_ERROR("NetworkSource: channel must be a valid UUID, got: {}", *value);
                return std::nullopt;
            }
            return value;
        }};

    static inline const DescriptorConfig::ConfigParameter<std::string> BIND{
        "bind",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            auto value = DescriptorConfig::tryGet(BIND, config);
            if (value && !EndpointValidation{}.isValid(*value))
            {
                NES_ERROR("NetworkSource: bind must be host:port format, got: {}", *value);
                return std::nullopt;
            }
            return value;
        }};

    /// Per-channel receiver queue size override. 0 means use the worker-level default.
    /// When a user explicitly sets receiver_queue_size=0, the lambda rejects it with an error.
    /// The default value (0) is returned directly by the config system, bypassing the lambda.
    static inline const DescriptorConfig::ConfigParameter<size_t> RECEIVER_QUEUE_SIZE{
        "receiver_queue_size",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<size_t>
        {
            auto value = DescriptorConfig::tryGet(RECEIVER_QUEUE_SIZE, config);
            if (value && *value == 0)
            {
                NES_ERROR("NetworkSource: receiver_queue_size must be > 0 when explicitly set");
                return std::nullopt;
            }
            return value;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, CHANNEL, BIND, RECEIVER_QUEUE_SIZE);
};

/// NOLINTEND(cert-err58-cpp)

}
