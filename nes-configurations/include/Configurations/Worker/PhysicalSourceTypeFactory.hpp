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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <yaml-cpp/yaml.h>

namespace NES
{

class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

namespace Configurations
{

class PhysicalSourceTypeFactory
{
public:
    /**
     * @brief create source config with yaml file and/or command line params
     * @param commandLineParams command line params
     * @param argc number of command line params
     * @return source config object
     */
    static PhysicalSourceTypePtr createFromString(std::string identifier, std::map<std::string, std::string>& inputParams);

    /**
     * @brief create physical source config with yaml file
     * @param physicalSourceConfig yaml elements from yaml file
     * @return physical source config object
     */
    static PhysicalSourceTypePtr createFromYaml(YAML::Node& yamlConfig);

private:
    static PhysicalSourceTypePtr createPhysicalSourceType(
        std::string logicalSourceName, std::string sourceType, const std::map<std::string, std::string>& commandLineParams);

    static PhysicalSourceTypePtr createPhysicalSourceType(std::string logicalSourceName, std::string sourceType, YAML::Node& yamlConfig);
};
} /// namespace Configurations
} /// namespace NES
