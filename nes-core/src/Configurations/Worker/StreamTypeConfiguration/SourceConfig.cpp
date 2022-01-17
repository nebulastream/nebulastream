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

#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalStreamConfig/SourceTypeConfig.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

namespace Configurations {

SourceTypeConfig::SourceTypeConfig(std::string _sourceType)
    :
      sourceType(
        ConfigurationOption<std::string>::create(SOURCE_TYPE_CONFIG,
                                                   std::move(_sourceType),
                                                   "Type of the Source (available options: NoSource, DefaultSource, CSVSource, "
                                                   "BinarySource, MQTTSource, KafkaSource, OPCSource).")) {
    NES_INFO("NesSourceConfig: Init source config object with new values.");
}

void SourceTypeConfig::resetSourceOptions(std::string _sourceType) {
    setSourceType(std::move(_sourceType));
}

std::string SourceTypeConfig::toString() {
    std::stringstream ss;
    ss << SOURCE_TYPE_CONFIG + ":" + sourceType->toStringNameCurrentValue();
    return ss.str();
}

bool SourceTypeConfig::equal(const SourceTypeConfigPtr& other) {
    if (!other->instanceOf<SourceTypeConfig>()) {
        return false;
    }
    return sourceType->getValue() == other->sourceType->getValue();
}

StringConfigOption SourceTypeConfig::getSourceType() const { return sourceType; }

void SourceTypeConfig::setSourceType(std::string sourceTypeValue) { sourceType->setValue(std::move(sourceTypeValue)); }

}// namespace Configurations
}// namespace NES