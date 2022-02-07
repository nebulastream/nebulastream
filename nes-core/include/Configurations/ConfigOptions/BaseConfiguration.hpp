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

#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGURATION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGURATION_HPP_
#include <Configurations/ConfigOptions/BaseOption.hpp>
#include <Configurations/ConfigOptions/EnumOption.hpp>
#include <Configurations/ConfigOptions/ScalarOption.hpp>
#include <Configurations/ConfigOptions/SequenceOption.hpp>
#include <Configurations/ConfigOptions/WrapOption.hpp>
#include <Exceptions/ConfigurationException.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <string>
namespace NES::Configurations {

/**
 * @brief This class is the bases for all configuration.
 * A configuration contains a set of config option as member fields.
 * An individual option could ether be defined as an root class, e.g., see CoordinatorConfiguration, in this case it would correspond to a dedicated YAML file.
 * Or it could be member field of a high level configuration, e.g., see OptimizerConfiguration.
 * To identify a member field, all configuration have to implement getOptionMap() and return a set of options.
 */
class BaseConfiguration : public BaseOption {
  public:
    /**
     * @brief Constructor for a root configuration. In this case the name and description are empty.
     */
    BaseConfiguration();

    /**
     * @brief Constructor for a nested configuration, which declares a specific name and description.
     * This is required for all nested configurations.
     * @param name of the configuration.
     * @param description of the configuration.
     */
    BaseConfiguration(const std::string& name, const std::string& description);
    virtual ~BaseConfiguration() = default;

    /**
     * @brief Overwrite the default configurations with those loaded from a YAML file.
     * @param filePath file path to the yaml file
     */
    void overwriteConfigWithYAMLFileInput(const std::string& filePath);

    /**
     * @brief Overwrite the default configurations with command line input.
     * @param inputParams map with key=command line parameter and value = value
     */
    void overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams);

    /**
     * @brief clears all options and set the default values
     */
    void clear() override;

    std::string toString();

  protected:
    void parseFromYAMLNode(const Yaml::Node config) override;
    void parseFromString(const std::string& identifier, const std::string& value) override;
    virtual std::vector<Configurations::BaseOption*> getOptions() = 0;
    std::map<std::string, Configurations::BaseOption*> getOptionMap();
};

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGURATION_HPP_
