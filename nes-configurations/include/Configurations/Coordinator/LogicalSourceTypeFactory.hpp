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

namespace NES::Configurations
{
using LogicalSourceTypePtr = std::shared_ptr<class LogicalSourceType>;

class LogicalSourceTypeFactory
{
public:
    static LogicalSourceTypePtr createFromString(std::string identifier, std::map<std::string, std::string>& inputParams);
    static LogicalSourceTypePtr createFromYaml(YAML::Node& yamlConfig);
};
}
