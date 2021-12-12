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

#include <Configurations/Sources/MaterializedViewSourceConfig.hpp>
#include <Configurations/ConfigOption.hpp>

namespace NES {

namespace Configurations {

MaterializedViewSourceConfigPtr MaterializedViewSourceConfig::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<MaterializedViewSourceConfig>(MaterializedViewSourceConfig(std::move(sourceConfigMap)));
}

MaterializedViewSourceConfigPtr MaterializedViewSourceConfig::create() {
    return std::make_shared<MaterializedViewSourceConfig>(MaterializedViewSourceConfig());
}

MaterializedViewSourceConfig::MaterializedViewSourceConfig(std::map<std::string, std::string> sourceConfigMap)
: SourceConfig(sourceConfigMap, MATERIALIZEDVIEW_SOURCE_CONFIG), id(ConfigOption<uint32_t>::create(MATERIALIZED_VIEW_ID_CONFIG,
                                      1,
                                      "id to identify the materialized view to read from")){
    NES_INFO("MaterializedViewSourceConfig: Init source config object with new values.");
    if (sourceConfigMap.find(MATERIALIZED_VIEW_ID_CONFIG) != sourceConfigMap.end()) {
        id->setValue(std::stoi(sourceConfigMap.find(MATERIALIZED_VIEW_ID_CONFIG)->second));
    } else {
        NES_THROW_RUNTIME_ERROR("MaterializedViewSourceConfig:: no id defined! Please define an id.");
    }
}

MaterializedViewSourceConfig::MaterializedViewSourceConfig() : SourceConfig(MATERIALIZEDVIEW_SOURCE_CONFIG), id(ConfigOption<uint32_t>::create(MATERIALIZED_VIEW_ID_CONFIG,
                                      1,
                                      "id to identify the materialized view to read from")){
    NES_INFO("MaterializedViewSourceConfig: Init source config object with default values.");
}

void MaterializedViewSourceConfig::resetSourceOptions() {
    setId(id->getDefaultValue());
    SourceConfig::resetSourceOptions(MATERIALIZEDVIEW_SOURCE_CONFIG);
}

std::string MaterializedViewSourceConfig::toString() {
    std::stringstream ss;
    ss << id->toStringNameCurrentValue();
    ss << SourceConfig::toString();
    return ss.str();
}

IntConfigOption MaterializedViewSourceConfig::getId() const { return id; }

void MaterializedViewSourceConfig::setId(uint32_t idValue) { id->setValue(idValue); }

}// namespace Configurations
}// namespace NES