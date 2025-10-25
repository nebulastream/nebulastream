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

#include <any>
#include <memory>
#include <sstream>
#include <string>
#include <typeinfo>
#include <utility>

#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <yaml-cpp/yaml.h>

namespace NES
{

/// @brief Template for a ConfigurationOption object
/// @tparam T template parameter, depends on ConfigOptions
template <class T>
class ConfigurationOption
{
public:
    static std::shared_ptr<ConfigurationOption> create(std::string name, T value, std::string description)
    {
        return std::make_shared<ConfigurationOption>(ConfigurationOption(name, value, description));
    };

    [[nodiscard]] std::string toString() const
    {
        std::stringstream ss;
        ss << "Name: " << name << "\n";
        ss << "Description: " << description << "\n";
        ss << "Value: " << value << "\n";
        ss << "Default Value: " << defaultValue << "\n";
        return ss.str();
    }

    /// @brief converts config object to human readable form, only prints name and current value
    [[nodiscard]] std::string toStringNameCurrentValue() const
    {
        std::stringstream ss;
        ss << name << ": " << value << "\n";
        return ss.str();
    }

    /// @brief converts config object to human readable form, only prints name and current value
    [[nodiscard]] std::string toStringNameCurrentValueEnum() const
    {
        std::stringstream ss;
        ss << name << ": " << magic_enum::enum_name(value) << "\n";
        return ss.str();
    }

    /// @brief converts the value of this object into a string
    [[nodiscard]] std::string getValueAsString() const { return std::to_string(value); };

    [[nodiscard]] const std::string& getName() const { return name; }

    [[nodiscard]] T getValue() const { return value; };

    void setValue(T value) { this->value = value; }

    void setValueIfDefined(YAML::Node yamlNode)
    {
        if (!yamlNode.IsNull())
        {
            this->value = yamlNode.as<T>();
        }
    }

    [[nodiscard]] std::string getDescription() const { return description; };

    [[nodiscard]] T getDefaultValue() const { return defaultValue; };

    bool equals(const std::any& other)
    {
        if (this == other)
        {
            return true;
        }
        if (other.has_value() && other.type() == typeid(ConfigurationOption))
        {
            auto that = (ConfigurationOption<T>)other;
            return this->name == that.name && this->description == that.description && this->value == that.value
                && this->defaultValue == that.defaultValue;
        }
        return false;
    };

private:
    explicit ConfigurationOption(std::string name, T value, std::string description)
        : name(std::move(name)), description(std::move(description)), value(value), defaultValue(value)
    {
    }

    ConfigurationOption(std::string name, T value, T defaultValue, std::string description)
        : name(std::move(name)), description(std::move(description)), value(value), defaultValue(defaultValue)
    {
    }

    std::string name;
    std::string description;
    T value;
    T defaultValue;
};


}
