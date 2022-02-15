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
#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_ENUMOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_ENUMOPTION_HPP_
#include <Configurations/ConfigOptions/TypedBaseOption.hpp>
#include <Exceptions/ConfigurationException.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <string>
#include <type_traits>

using namespace magic_enum::ostream_operators;
namespace NES::Configurations {
/**
 * @brief This class defines an option, which has only the member of an enum as possible values.
 * @tparam EnumType
 */
template<class EnumType>
requires std::is_enum<EnumType>::value class EnumOption : public TypedBaseOption<EnumType> {
  public:
    /**
     * @brief Constructor to define a EnumOption with a specific default value.
     * @param name of the EnumOption.
     * @param defaultValue of the EnumOption, has to be an member of the EnumType.
     * @param description of the EnumOption.
     */
    EnumOption(const std::string& name, EnumType defaultValue, const std::string& description);

    /**
     * @brief Operator to assign a new value as a value of this option.
     * @param value that will be assigned
     * @return Reference to this option.
     */
    EnumOption<EnumType>& operator=(const EnumType& value);
    std::string toString() override;

  protected:
    void parseFromYAMLNode(Yaml::Node node) override;
    void parseFromString(std::string identifier, std::map<std::string, std::string>& inputParams) override;
};

template<class EnumType>
requires std::is_enum<EnumType>::value
EnumOption<EnumType>::EnumOption(const std::string& name, EnumType defaultValue, const std::string& description)
    : TypedBaseOption<EnumType>(name, defaultValue, description){};

template<class EnumType>
requires std::is_enum<EnumType>::value EnumOption<EnumType>
&EnumOption<EnumType>::operator=(const EnumType& value) {
    this->value = value;
    return *this;
}

template<class EnumType>
requires std::is_enum<EnumType>::value void EnumOption<EnumType>::parseFromYAMLNode(Yaml::Node node) {

    if (!magic_enum::enum_contains<EnumType>(node.As<std::string>())) {
        auto name = std::string(magic_enum::enum_names_to_string<EnumType>());
        throw Exceptions::ConfigurationException("Enum for " + node.As<std::string>() + " was not found. Valid options are " + name);
    }
    this->value = magic_enum::enum_cast<EnumType>(node.As<std::string>()).value();
}

template<class EnumType>
requires std::is_enum<EnumType>::value void
EnumOption<EnumType>::parseFromString(std::string identifier, std::map<std::string, std::string>& inputParams) {
    auto value = inputParams[identifier];
    // Check if the value is a member of this enum type.
    if (!magic_enum::enum_contains<EnumType>(value)) {
        auto name = std::string(magic_enum::enum_names_to_string<EnumType>());
        throw Exceptions::ConfigurationException("Enum for " + value + " was not found. Valid options are " + name);
    }
    this->value = magic_enum::enum_cast<EnumType>(value).value();
}

template<class EnumType>
requires std::is_enum<EnumType>::value
    std::string EnumOption<EnumType>::toString() {
    return "";
}

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_ENUMOPTION_HPP_
