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

#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_LOGICALSOURCEFACTORY_HPP_
#define NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_LOGICALSOURCEFACTORY_HPP_

#include <Util/yaml/Yaml.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NES::Configurations {

class LogicalSourceType;
using LogicalSourceTypePtr = std::shared_ptr<LogicalSourceType>;

class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;

class LogicalSourceTypeFactory {
  public:
    /**
     * Create logical source config from string parameters (yaml/cli)
     * @param identifier
     * @param inputParams
     */
    static LogicalSourceTypePtr createFromString(std::string identifier, std::map<std::string, std::string>& inputParams);

    /**
     * @brief create logical stream config with yaml file
     * @param logicalStreamConfig yaml elements from yaml file
     * @return physical stream config object
     */
    static LogicalSourceTypePtr createFromYaml(Yaml::Node& yamlConfig);
};
}// namespace NES::Configurations

#endif// NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_LOGICALSOURCEFACTORY_HPP_
