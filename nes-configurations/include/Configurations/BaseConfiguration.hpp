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
#include <string>
#include <Configurations/BaseOption.hpp>
#include <Configurations/SequenceOption.hpp>
#include <Identifiers/Identifiers.hpp>
#include <yaml-cpp/yaml.h>

namespace NES::Configurations
{

/// This class is the bases for all configuration.
/// A configuration contains a set of config option as member fields.
/// An individual option could ether be defined as an root class, e.g., see CoordinatorConfiguration, in this case it would correspond to a dedicated YAML file.
/// Or it could be member field of a high level configuration, e.g., see OptimizerConfiguration.
/// To identify a member field, all configuration have to implement getOptionMap() and return a set of options.
class BaseConfiguration : public BaseOption
{
public:
    BaseConfiguration(const std::string& name, const std::string& description);
    BaseConfiguration();
    ~BaseConfiguration() override = default;

    void overwriteConfigWithYAMLFileInput(const std::string& filePath);

    /// @param inputParams map with key=command line parameter and value = value
    void overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams);

    /// @param withOverwrite false if workerId is not in yaml file, true if it is and has to be changed
    bool persistWorkerIdInYamlConfigFile(std::string yamlFilePath, WorkerId workerId, bool withOverwrite);

    /// clears all options and set the default values
    void clear() override;

    std::string toString() override;

protected:
    void parseFromYAMLNode(const YAML::Node config) override;
    void parseFromString(std::string identifier, std::map<std::string, std::string>& inputParams) override;
    virtual std::vector<BaseOption*> getOptions() = 0;
    std::map<std::string, BaseOption*> getOptionMap();
};
}
