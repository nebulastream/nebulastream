/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Configurations/Sources/DefaultSourceConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

DefaultSourceConfigPtr DefaultSourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<DefaultSourceConfig>(DefaultSourceConfig(std::move(sourceConfigMap)));
}

DefaultSourceConfigPtr DefaultSourceConfig::create() { return std::make_shared<DefaultSourceConfig>(DefaultSourceConfig()); }

DefaultSourceConfig::DefaultSourceConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceConfig(std::move(sourceConfigMap), DEFAULT_SOURCE_CONFIG) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

DefaultSourceConfig::DefaultSourceConfig() : SourceConfig(DEFAULT_SOURCE_CONFIG) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

void DefaultSourceConfig::resetSourceOptions() { SourceConfig::resetSourceOptions(DEFAULT_SOURCE_CONFIG); }

std::string DefaultSourceConfig::toString() {
    std::stringstream ss;
    ss << SourceConfig::toString();
    return ss.str();
}
}// namespace Configurations
}// namespace NES