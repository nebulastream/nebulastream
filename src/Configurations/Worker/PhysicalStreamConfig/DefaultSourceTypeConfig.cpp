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

#include <Configurations/ConfigOption.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/DefaultSourceTypeConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

DefaultSourceTypeConfigPtr DefaultSourceTypeConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<DefaultSourceTypeConfig>(DefaultSourceTypeConfig(std::move(sourceConfigMap)));
}

DefaultSourceTypeConfigPtr DefaultSourceTypeConfig::create() {
    return std::make_shared<DefaultSourceTypeConfig>(DefaultSourceTypeConfig());
}

DefaultSourceTypeConfig::DefaultSourceTypeConfig(std::map<std::string, std::string> sourceConfigMap)
    : SourceTypeConfig(std::move(sourceConfigMap), DEFAULT_SOURCE_CONFIG),
      numberOfBuffersToProduce(
          ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

DefaultSourceTypeConfig::DefaultSourceTypeConfig()
    : SourceTypeConfig(DEFAULT_SOURCE_CONFIG),
      numberOfBuffersToProduce(
          ConfigOption<uint32_t>::create(NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG, 1, "Number of buffers to produce.")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

void DefaultSourceTypeConfig::resetSourceOptions() { SourceTypeConfig::resetSourceOptions(DEFAULT_SOURCE_CONFIG); }

std::string DefaultSourceTypeConfig::toString() {
    std::stringstream ss;
    ss << SourceTypeConfig::toString();
    return ss.str();
}

bool DefaultSourceTypeConfig::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<DefaultSourceTypeConfig>()) {
        return false;
    }
    auto otherSourceConfig = other->as<DefaultSourceTypeConfig>();
    return SourceTypeConfig::equal(other);
}

}// namespace Configurations
}// namespace NES