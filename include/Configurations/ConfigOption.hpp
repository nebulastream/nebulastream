/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTION_HPP_
#define NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTION_HPP_

#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <any>
#include <sstream>
#include <string>
#include <typeinfo>
#include <utility>

namespace NES {

/**
 * @brief Template for a ConfigOption object
 * @tparam T template parameter, depends on ConfigOptions
 */
template<class T>
class ConfigOption {
  public:
    static std::shared_ptr<ConfigOption> create(std::string name, T value, std::string description) {
        return std::make_shared<ConfigOption>(ConfigOption(name, value, description));
    };

    static std::shared_ptr<ConfigOption> create(std::string name, T value, T defaultValue, std::string description) {
        return std::make_shared<ConfigOption>(ConfigOption(name, value, defaultValue, description));
    };

    /**
     * @brief converts a ConfigOption Object into human readable format
     * @return string representation of the config
     */
    std::string toString() {
        std::stringstream ss;
        ss << "Config Object: \n";
        ss << "Name: " << name << "\n";
        ss << "Description: " << description << "\n";
        ss << "Value: " << value << "\n";
        ss << "Default Value: " << defaultValue << "\n";
        return ss.str();
    }

    /**
     * @brief converts config object to human readable form, only prints name and current value
     * @return Name: current Value of config object
     */
    std::string toStringNameCurrentValue(){
        std::stringstream ss;
        ss << name << ": " << value << "\n";
        return ss.str();
    }

    /**
     * @brief converts the value of this object into a string
     * @return string of the value of this object
     */
    [[nodiscard]] std::string getValueAsString() const { return string(value); };

    /**
     * @brief get the name of the ConfigOption Object
     * @return name of the config
     */
    std::string getName() { return name; }

    /**
     * @brief get the value of the ConfigOption Object
     * @return the value of the config if not set then default value
     */
    [[nodiscard]] T getValue() const { return value; };

    /**
     * @brief sets the value
     * @param value: the value to be used
     */
    void setValue(T value) { this->value = value; }

    /**
     * @brief get the description of this parameter
     * @return description of the config
     */
    [[nodiscard]] std::string getDescription() const { return description; };

    /**
     * @brief get the default value of this parameter
     * @return: the default value
     */
    [[nodiscard]] T getDefaultValue() const { return defaultValue; };

    /**
     * @brief perform equality check between two config options
     * @param other: other config option
     * @return true if equal else return false
     */
    bool equals(const std::any& other) {
        if (this == other) {
            return true;
        }
        if (other.has_value() && other.type() == typeid(ConfigOption)) {
            auto that = (ConfigOption<T>) other;
            return this->name == that.name && this->description == that.description && this->value == that.value
                && this->defaultValue == that.defaultValue;
        }
        return false;
    };

  private:
    /**
     * @brief Constructs a ConfigOption<T> object
     * @param name the name of the object
     * @param value the value of the object
     * @param description default value of the object
     */
    explicit ConfigOption(std::string name, T value, std::string description)
        : name(std::move(name)), description(std::move(description)), value(value), defaultValue(value) {}

    ConfigOption(std::string name, T value, T defaultValue, std::string description)
        : name(std::move(name)), description(std::move(description)), value(value), defaultValue(defaultValue) {}

    std::string name;
    std::string description;
    T value;
    T defaultValue;
};
}// namespace NES

#endif// NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTION_HPP_
