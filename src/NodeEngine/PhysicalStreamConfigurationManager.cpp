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

#include <NodeEngine/PhysicalStreamConfigurationManager.hpp>

namespace NES::NodeEngine {

PhysicalStreamConfigurationManager::PhysicalStreamConfigurationManager(
    PhysicalStreamsPersistencePtr configurationPersistence,
    const std::vector<PhysicalStreamConfigPtr>& initialPhysicalStreamConfigs)
    : configurationPersistence(std::move(configurationPersistence)) {

    for (auto const& physicalStreamConfig : initialPhysicalStreamConfigs) {
        this->addPhysicalStreamConfig(physicalStreamConfig);
    }
}

AbstractPhysicalStreamConfigPtr
PhysicalStreamConfigurationManager::getPhysicalStreamConfig(const std::string& physicalStreamName) {
    if (physicalStreams.find(physicalStreamName) != physicalStreams.end()) {
        return physicalStreams[physicalStreamName];
    } else {
        NES_ERROR("NodeEngine::getConfig: physical stream does not exists " << physicalStreamName);
        throw Exception("Required stream does not exists " + physicalStreamName);
    }
}

AbstractPhysicalStreamConfigPtr
PhysicalStreamConfigurationManager::getPhysicalStreamConfigForSourceDescriptorName(const std::string& sourceDescriptorName) {
    for (auto const& [physicalStreamName, conf] : physicalStreams) {
        auto logicalStreamNames = conf->getLogicalStreamNames();
        for (std::string logicalStreamName : logicalStreamNames) {
            if (logicalStreamName == sourceDescriptorName) {
                return conf;
            }
        }
    }
    return NULL;
}
bool PhysicalStreamConfigurationManager::addPhysicalStreamConfig(AbstractPhysicalStreamConfigPtr physicalStreamConfig) {
    this->physicalStreams[physicalStreamConfig->getPhysicalStreamName()] = physicalStreamConfig;
    return this->configurationPersistence->persistConfiguration(physicalStreamConfig->toSourceConfig());
}
PhysicalStreamConfigurationManager::~PhysicalStreamConfigurationManager() {
    // nop
}
}// namespace NES::NodeEngine
