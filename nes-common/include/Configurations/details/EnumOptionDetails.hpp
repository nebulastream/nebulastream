
#ifndef NES_NES_COMMON_INCLUDE_CONFIGURATIONS_DETAILS_ENUMOPTIONDETAILS_HPP_
#define NES_NES_COMMON_INCLUDE_CONFIGURATIONS_DETAILS_ENUMOPTIONDETAILS_HPP_

#include <Util/magicenum/magic_enum.hpp>
#include <Configurations/EnumOption.hpp>

using namespace magic_enum::ostream_operators;
namespace NES::Configurations {


template<class EnumType>
    requires std::is_enum<EnumType>::value
EnumOption<EnumType>::EnumOption(const std::string& name, EnumType defaultValue, const std::string& description)
    : TypedBaseOption<EnumType>(name, defaultValue, description){};

template<class EnumType>
    requires std::is_enum<EnumType>::value
EnumOption<EnumType>& EnumOption<EnumType>::operator=(const EnumType& value) {
    this->value = value;
    return *this;
}

template<class EnumType>
    requires std::is_enum<EnumType>::value
void EnumOption<EnumType>::parseFromYAMLNode(Yaml::Node node) {

    if (!magic_enum::enum_contains<EnumType>(node.As<std::string>())) {
        std::stringstream ss;
        for(const auto& name: magic_enum::enum_names<EnumType>()){
            ss << name;
        }
        throw ConfigurationException("Enum for " + node.As<std::string>() + " was not found. Valid options are " + ss.str());
    }
    this->value = magic_enum::enum_cast<EnumType>(node.As<std::string>()).value();
}

template<class EnumType>
    requires std::is_enum<EnumType>::value
void EnumOption<EnumType>::parseFromString(std::string identifier, std::map<std::string, std::string>& inputParams) {
    auto value = inputParams[identifier];
    // Check if the value is a member of this enum type.
    if (!magic_enum::enum_contains<EnumType>(value)) {
        std::stringstream ss;
        for(const auto& name: magic_enum::enum_names<EnumType>()){
            ss << name;
        }
        throw ConfigurationException("Enum for " + value + " was not found. Valid options are " + ss.str());
    }
    this->value = magic_enum::enum_cast<EnumType>(value).value();
}

template<class EnumType>
    requires std::is_enum<EnumType>::value
std::string EnumOption<EnumType>::toString() {
    return "";
}

}

#endif//NES_NES_COMMON_INCLUDE_CONFIGURATIONS_DETAILS_ENUMOPTIONDETAILS_HPP_
