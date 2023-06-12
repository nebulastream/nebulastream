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

namespace NES {

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

namespace Configurations {

class LogicalSourceFactory {
  public:
    /**
     * Create logical source config from string parameters (yaml/cli)
     * @param identifier
     * @param inputParams
     */
    static LogicalSourcePtr createFromString(std::string identifier, std::map<std::string, std::string>& inputParams);

    /**
     * @brief create logical stream config with yaml file
     * @param logicalStreamConfig yaml elements from yaml file
     * @return physical stream config object
     */
    static LogicalSourcePtr createFromYaml(Yaml::Node& yamlConfig);

  private:
    /**
     * Return the appropriate NES type from yaml string configuration. Ignores
     * fieldLength if it doesn't make sense, errors length is missing and type
     * is string.
     * @param fieldType the type of the schema field from yaml
     * @param fieldLength the length of the field from yaml
     * @return the appropriate DataTypePtr
     */
    static DataTypePtr stringToFieldType(std::string fieldType, std::string fieldLength);
};
}// namespace Configurations
}// namespace NES

#endif// NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_LOGICALSOURCEFACTORY_HPP_
