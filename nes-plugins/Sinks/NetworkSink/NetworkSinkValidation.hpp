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
#include <Sinks/SinkDescriptor.hpp>

namespace NES
{

/// NOLINTBEGIN(cert-err58-cpp)
struct ConfigParametersNetworkSink
{
    static inline const DescriptorConfig::ConfigParameter<std::string> DATA_ENDPOINT{
        "data_endpoint",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DATA_ENDPOINT, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> BIND{
        "bind",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BIND, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "channel",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHANNEL, config); }};

    static inline const DescriptorConfig::ConfigParameter<size_t> BACKPRESSURE_UPPER_THRESHOLD{
        "backpressure_upper_threshold",
        1000,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(BACKPRESSURE_UPPER_THRESHOLD, config); }};

    static inline const DescriptorConfig::ConfigParameter<size_t> BACKPRESSURE_LOWER_THRESHOLD{
        "backpressure_lower_threshold",
        200,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(BACKPRESSURE_LOWER_THRESHOLD, config); }};

    /// Per-channel sender queue size override. 0 means use the worker-level default.
    static inline const DescriptorConfig::ConfigParameter<size_t> SENDER_QUEUE_SIZE{
        "sender_queue_size",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SENDER_QUEUE_SIZE, config); }};

    /// Per-channel max pending acks override. 0 means use the worker-level default.
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_PENDING_ACKS{
        "max_pending_acks",
        size_t{0},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MAX_PENDING_ACKS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap,
            DATA_ENDPOINT,
            CHANNEL,
            BIND,
            BACKPRESSURE_UPPER_THRESHOLD,
            BACKPRESSURE_LOWER_THRESHOLD,
            SENDER_QUEUE_SIZE,
            MAX_PENDING_ACKS);
};

/// NOLINTEND(cert-err58-cpp)

}
