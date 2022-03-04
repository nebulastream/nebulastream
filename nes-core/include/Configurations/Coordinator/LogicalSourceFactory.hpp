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

#ifndef NES_INCLUDE_CONFIGURATIONS_COORDINATOR_LOGICALSOURCEFACTORY_HPP_
#define NES_INCLUDE_CONFIGURATIONS_COORDINATOR_LOGICALSOURCEFACTORY_HPP_

// TODO: check why yaml is not needed in equivalent physical file structure
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

namespace Configurations {

class LogicalSourceFactory {
  public:
    /**
     * @brief create logical stream config with yaml file
     * @param logicalStreamConfig yaml elements from yaml file
     * @return physical stream config object
     */
    static void createFromYaml(Yaml::Node& yamlConfig);

  private:
    /**
     * @brief Compute Physical Source Type based on the yaml configuration
     * @param sourceType : the string representing the physical source type
     * @param yamlConfig : the yaml configuration
     * @return PhysicalSourceType shared pointer
     */
    static LogicalSourcePtr createPhysicalSourceType(std::string sourceType, Yaml::Node& yamlConfig);
};
}// namespace Configurations
}// namespace NES

#endif//NES_INCLUDE_CONFIGURATIONS_COORDINATOR_LOGICALSOURCEFACTORY_HPP_
