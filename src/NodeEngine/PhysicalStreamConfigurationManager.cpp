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

#include "../../include/NodeEngine/PhysicalStreamConfigurationManager.hpp"

namespace NES::NodeEngine {

PhysicalStreamConfigurationManager::PhysicalStreamConfigurationManager() {}

PhysicalStreamConfigurationManager::PhysicalStreamConfigurationManager(
    const std::vector<PhysicalStreamConfigPtr>& initialPhysicalStreamConfigs) {
    for (auto const& physicalStreamConfig : initialPhysicalStreamConfigs){
        this->physicalStreams[physicalStreamConfig]
    }
}

PhysicalStreamConfigurationManagerPtr PhysicalStreamConfigurationManager::create() {
    return std::make_shared<PhysicalStreamConfigurationManager>();
}

PhysicalStreamConfigPtr PhysicalStreamConfigurationManager::getPhysicalStreamConfig(const std::string& physicalStreamName) {

    return NULL;
}

std::vector<PhysicalStreamConfigPtr>
PhysicalStreamConfigurationManager::getPhysicalStreamConfigByLogicalName(const std::string& logicalStreamName) {
    return NULL;
}
}// namespace NES::NodeEngine
