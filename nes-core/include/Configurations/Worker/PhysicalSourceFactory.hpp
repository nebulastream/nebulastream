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

#ifndef NES_PHYSICALSOURCEFACTORY_HPP
#define NES_PHYSICALSOURCEFACTORY_HPP

#include <Util/yaml/rapidyaml.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace Configurations {

class PhysicalSourceFactory {
  public:
    /**
     * @brief create source config with yaml file and/or command line params
     * @param commandLineParams command line params
     * @param argc number of command line params
     * @return source config object
     */
    static std::vector<PhysicalSourcePtr> createSourceConfig(const std::map<std::string, std::string>& commandLineParams);

    /**
     * @brief create physical stream config with yaml file
     * @param physicalStreamConfig yaml elements from yaml file
     * @return physical stream config object
     */
    static std::vector<PhysicalSourcePtr> createSourceConfig(ryml::NodeRef sourceTypeConfigNode, std::string sourceType);

    /**
     * @brief create default source config with default values of type sourceType
     * @param sourceType source type of source config object
     * @return source config object of type sourceType
     */
    static PhysicalSourcePtr createSourceConfig(std::string sourceType);

    /**
     * @brief create default source config
     * @return default source config object
     */
    static PhysicalSourcePtr createSourceConfig();
};
}// namespace Configurations
}// namespace NES

#endif//NES_PHYSICALSOURCEFACTORY_HPP
