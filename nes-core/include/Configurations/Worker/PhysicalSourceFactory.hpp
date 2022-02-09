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

#ifndef NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCEFACTORY_HPP_
#define NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCEFACTORY_HPP_

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

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
    static PhysicalSourcePtr createFromString(std::string identifier,
                                              std::map<std::string, std::string>& inputParams);

    /**
     * @brief create physical stream config with yaml file
     * @param physicalStreamConfig yaml elements from yaml file
     * @return physical stream config object
     */
    static PhysicalSourcePtr createFromYaml(Yaml::Node& yamlConfig);



  private:
    /**
     * @brief Compute Physical Source Type based on the command line arguments
     * @param sourceType : the string representing the physical source type
     * @param commandLineParams : the command line arguments
     * @return PhysicalSourceType shared pointer
     */
    static PhysicalSourceTypePtr createPhysicalSourceType(std::string sourceType,
                                                          const std::map<std::string, std::string>& commandLineParams);

    /**
     * @brief Compute Physical Source Type based on the yaml configuration
     * @param sourceType : the string representing the physical source type
     * @param yamlConfig : the yaml configuration
     * @return PhysicalSourceType shared pointer
     */
    static PhysicalSourceTypePtr createPhysicalSourceType(std::string sourceType, Yaml::Node& yamlConfig);
};
}// namespace Configurations
}// namespace NES

#endif// NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICALSOURCEFACTORY_HPP_
