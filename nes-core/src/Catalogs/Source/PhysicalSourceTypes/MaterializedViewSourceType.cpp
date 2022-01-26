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

#include <Catalogs/Source/PhysicalSourceTypes/MaterializedViewSourceType.hpp>

namespace NES {

namespace Configurations {

namespace Experimental::MaterializedView {

MaterializedViewSourceTypePtr MaterializedViewSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<MaterializedViewSourceType>(MaterializedViewSourceType(std::move(sourceConfigMap)));
}

MaterializedViewSourceTypePtr MaterializedViewSourceType::create() {
    return std::make_shared<MaterializedViewSourceType>(MaterializedViewSourceType());
}

MaterializedViewSourceType::MaterializedViewSourceType(std::map<std::string, std::string> sourceConfigMap)
    : MaterializedViewSourceType() {
    NES_INFO("MaterializedViewSourceType: Init source config object with new values.");
    if (sourceConfigMap.find(MATERIALIZED_VIEW_ID_CONFIG) != sourceConfigMap.end()) {
        id->setValue(std::stoi(sourceConfigMap.find(MATERIALIZED_VIEW_ID_CONFIG)->second));
    } else {
        NES_THROW_RUNTIME_ERROR("MaterializedViewSourceType:: no id defined! Please define an id.");
    }
}

MaterializedViewSourceType::MaterializedViewSourceType()
    : PhysicalSourceType(MATERIALIZEDVIEW_SOURCE),
      id(Configurations::ConfigurationOption<uint32_t>::create(MATERIALIZED_VIEW_ID_CONFIG,
                                                               1,
                                                               "id to identify the materialized view to read from")) {
    NES_INFO("MaterializedViewSourceType: Init source config object with default values.");
}

void MaterializedViewSourceType::reset() { setId(id->getDefaultValue()); }

std::string MaterializedViewSourceType::toString() {
    std::stringstream ss;
    ss << id->toStringNameCurrentValue();
    return ss.str();
}

IntConfigOption MaterializedViewSourceType::getId() const { return id; }

void MaterializedViewSourceType::setId(uint32_t idValue) { id->setValue(idValue); }

bool MaterializedViewSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<MaterializedViewSourceType>()) {
        return false;
    }
    auto otherSourceType = other->as<MaterializedViewSourceType>();
    return id == otherSourceType->id;
}
}// namespace Experimental::MaterializedView
}// namespace Configurations
}// namespace NES