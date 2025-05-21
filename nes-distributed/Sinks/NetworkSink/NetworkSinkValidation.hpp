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
#include <Configurations/Descriptor.hpp>

namespace NES::Sinks
{

static constexpr std::string_view NETWORK_SINK_NAME = "Network";

/// TODO #355 : combine configuration with source configuration (get rid of duplicated code)
struct ConfigParametersNetworkSink
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> CONNECTION{
        "connection",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(CONNECTION, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> CHANNEL{
        "channel",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(CHANNEL, config); }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(CHANNEL, CONNECTION);
};
}
