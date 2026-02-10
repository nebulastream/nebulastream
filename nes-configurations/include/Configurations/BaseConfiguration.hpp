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

#include <string>
#include <unordered_map>
#include <Configurations/BaseOption.hpp>
#include <Configurations/SequenceOption.hpp>
#include <Identifiers/Identifiers.hpp>
#include <yaml-cpp/yaml.h>

namespace NES
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
    BaseConfiguration() = default;
    ~BaseConfiguration() override = default;

    void overwriteConfigWithYAMLFileInput(const std::string& filePath);

    /// @param inputParams map with key=command line parameter and value = value
    void overwriteConfigWithCommandLineInput(const std::unordered_map<std::string, std::string>& inputParams);

    /**
     * @brief Overwrite configuration from a YAML node
     * @param node YAML node containing configuration key-value pairs
     */
    void overwriteConfigWithYAMLNode(const YAML::Node& node);

    /// clears all options and set the default values
    void clear() override;

    void accept(OptionVisitor& visitor) override;

    /// Returns true if any child option was explicitly set.
    [[nodiscard]] bool isExplicitlySet() const override;

    /// Copies only explicitly-set options from source onto this configuration.
    /// For nested BaseConfiguration children, recurses. For leaf options, copies the value.
    void applyExplicitlySetFrom(const BaseConfiguration& source);

    void copyValueFrom(const BaseOption& source) override;

protected:
    template <DerivedBaseOption T>
    friend class SequenceOption;

    void parseFromYAMLNode(const YAML::Node config) override;
    void parseFromString(std::string identifier, std::unordered_map<std::string, std::string>& inputParams) override;

    virtual std::vector<BaseOption*> getOptions() = 0;

    /// Const version of getOptions(). By default delegates to the non-const version.
    /// Override this if the configuration needs to return genuinely const options.
    [[nodiscard]] virtual std::vector<const BaseOption*> getOptions() const;

    std::unordered_map<std::string, BaseOption*> getOptionMap();
};
}
