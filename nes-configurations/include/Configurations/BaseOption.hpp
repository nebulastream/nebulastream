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
#include <yaml-cpp/yaml.h>

namespace NES
{

class OptionVisitor;

/// This class is the basis of all options. All options can define a name and a description.
class BaseOption
{
public:
    BaseOption() = default;

    /// Constructor to create a new option.
    BaseOption(const std::string& name, const std::string& description);
    virtual ~BaseOption() = default;

    /// Clears the option and sets a default value if available.
    virtual void clear() = 0;

    /// Checks if the option is equal to another option.
    virtual bool operator==(const BaseOption& other);

    std::string getName();

    std::string getDescription();

    virtual void accept(OptionVisitor&) = 0;

    /// Returns true if this option was explicitly set by user input (YAML, CLI, or setValue),
    /// as opposed to just holding its default value.
    [[nodiscard]] virtual bool isExplicitlySet() const { return explicitlySet_; }

    /// Copies the value from another option of the same concrete type.
    virtual void copyValueFrom(const BaseOption& source) = 0;

protected:
    friend class BaseConfiguration;

    /// ParseFromYamlNode fills the content of this option with the value of the YAML node.
    virtual void parseFromYAMLNode(YAML::Node node) = 0;

    /// ParseFromString fills the content of this option with a specific string value.
    /// If this option is nested it uses the identifier to lookup the particular children option.
    virtual void parseFromString(std::string identifier, std::unordered_map<std::string, std::string>& inputParams) = 0;

    std::string name;
    std::string description;
    bool explicitlySet_ = false;
};

template <class T>
concept DerivedBaseOption = std::is_base_of_v<BaseOption, T>;

}
