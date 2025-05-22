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
#include <type_traits>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/OptionVisitor.hpp>
#include <Configurations/TypedBaseOption.hpp>
#include <magic_enum/magic_enum.hpp>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>

namespace NES::Configurations
{

template <class T>
concept IsEnum = std::is_enum<T>::value;

/// This class defines an option, which has only the member of an enum as possible values.
template <IsEnum T>
class EnumOption : public TypedBaseOption<T>
{
public:
    /// Constructor to define a EnumOption with a specific default value.
    EnumOption(const std::string& name, T defaultValue, const std::string& description)
        : TypedBaseOption<T>(name, defaultValue, description) { };

    /// Operator to assign a new value as a value of this option.
    EnumOption<T>& operator=(const T& value)
    {
        this->value = value;
        return *this;
    };

    std::string toString() override
    {
        std::stringstream os;
        os << "Name: " << this->name << "\n";
        os << "Description: " << this->description << "\n";
        os << "Value: " << std::string(magic_enum::enum_name(this->value)) << "\n";
        os << "Default Value: " << std::string(magic_enum::enum_name(this->defaultValue)) << "\n";
        return os.str();
    };

    void accept(OptionVisitor& visitor) override
    {
        auto* config = dynamic_cast<Configurations::BaseConfiguration*>(this);
        visitor.visitConcrete(this->name, this->description, magic_enum::enum_name(this->getDefaultValue()));
        if (config)
        {
            config->accept(visitor);
        }
    }


protected:
    void parseFromYAMLNode(YAML::Node node) override
    {
        if (!magic_enum::enum_contains<T>(node.as<std::string>()))
        {
            std::stringstream ss;
            for (const auto& name : magic_enum::enum_names<T>())
            {
                ss << name;
            }
            throw InvalidConfigParameter("Enum for {} was not found. Valid options are {}", node.as<std::string>(), ss.str());
        }
        this->value = magic_enum::enum_cast<T>(node.as<std::string>()).value();
    };

    void parseFromString(std::string identifier, std::unordered_map<std::string, std::string>& inputParams) override
    {
        auto value = inputParams[identifier];
        /// Check if the value is a member of this enum type.
        if (!magic_enum::enum_contains<T>(value))
        {
            std::stringstream ss;
            for (const auto& name : magic_enum::enum_names<T>())
            {
                ss << name;
            }
            throw InvalidConfigParameter("Enum for {} was not found. Valid options are {}", value, ss.str());
        }
        this->value = magic_enum::enum_cast<T>(value).value();
    };
};

}
