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

#ifndef NES_PHYSICALSTREAMCONFIGURATIONMANAGER_H
#define NES_PHYSICALSTREAMCONFIGURATIONMANAGER_H

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Configurations/Persistence/PhysicalStreamsPersistence.hpp>
#include <vector>

namespace NES::NodeEngine {

/**
 * @brief: This class manages all physical stream configurations of a node
 */
class PhysicalStreamConfigurationManager {
  public:
    PhysicalStreamConfigurationManager() = delete;
    PhysicalStreamConfigurationManager(const QueryManager&) = delete;
    PhysicalStreamConfigurationManager& operator=(const QueryManager&) = delete;

    explicit PhysicalStreamConfigurationManager(PhysicalStreamsPersistencePtr configurationPersistence,
                                                const std::vector<PhysicalStreamConfigPtr>& initialPhysicalStreamConfigs);

    ~PhysicalStreamConfigurationManager();

    AbstractPhysicalStreamConfigPtr getPhysicalStreamConfig(const std::string& physicalStreamName);

    bool addPhysicalStreamConfig(AbstractPhysicalStreamConfigPtr physicalStreamConfig);

    AbstractPhysicalStreamConfigPtr getPhysicalStreamConfigForSourceDescriptorName(const std::string& sourceDescriptorName);

  private:
    PhysicalStreamsPersistencePtr configurationPersistence;
    std::map<std::string, AbstractPhysicalStreamConfigPtr> physicalStreams;
};

typedef std::shared_ptr<PhysicalStreamConfigurationManager> PhysicalStreamConfigurationManagerPtr;
}// namespace NES::NodeEngine
#endif//NES_PHYSICALSTREAMCONFIGURATIONMANAGER_H
