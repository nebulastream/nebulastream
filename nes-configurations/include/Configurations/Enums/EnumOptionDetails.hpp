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
#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_ENUMS_ENUMOPTIONDETAILS_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_ENUMS_ENUMOPTIONDETAILS_HPP_

#include "Configurations/Enums/EnumOption.hpp"
#include "Util/magicenum/magic_enum.hpp"
#include "Util/yaml/Yaml.hpp"

using namespace magic_enum::ostream_operators;
namespace NES::Configurations {

template<IsEnum T>
EnumOption<T>::EnumOption(const std::string& name, T defaultValue, const std::string& description)
    : TypedBaseOption<T>(name, defaultValue, description){};

template<IsEnum T>
EnumOption<T>& EnumOption<T>::operator=(const T& value) {
    this->value = value;
    return *this;
}

template<IsEnum T>
void EnumOption<T>::parseFromYAMLNode(Yaml::Node node) {

    if (!magic_enum::enum_contains<T>(node.As<std::string>())) {
        std::stringstream ss;
        for (const auto& name : magic_enum::enum_names<T>()) {
            ss << name;
        }
        throw ConfigurationException("Enum for " + node.As<std::string>() + " was not found. Valid options are " + ss.str());
    }
    this->value = magic_enum::enum_cast<T>(node.As<std::string>()).value();
}

template<IsEnum T>
void EnumOption<T>::parseFromString(std::string identifier, std::map<std::string, std::string>& inputParams) {
    auto value = inputParams[identifier];
    // Check if the value is a member of this enum type.
    if (!magic_enum::enum_contains<T>(value)) {
        std::stringstream ss;
        for (const auto& name : magic_enum::enum_names<T>()) {
            ss << name;
        }
        throw ConfigurationException("Enum for " + value + " was not found. Valid options are " + ss.str());
    }
    this->value = magic_enum::enum_cast<T>(value).value();
}

template<IsEnum T>
std::string EnumOption<T>::toString() {
    std::stringstream os;
    os << "Name: " << this->name << "\n";
    os << "Description: " << this->description << "\n";
    os << "Value: " << std::string(magic_enum::enum_name(this->value)) << "\n";
    os << "Default Value: " << std::string(magic_enum::enum_name(this->defaultValue)) << "\n";
    return os.str();
}

}// namespace NES::Configurations

#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_ENUMS_ENUMOPTIONDETAILS_HPP_
