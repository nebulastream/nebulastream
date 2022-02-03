#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_ENUMOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_ENUMOPTION_HPP_
#include <Configurations/ConfigOptions/TypedBaseOption.hpp>
#include <Exceptions/ConfigurationException.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <string>
#include <type_traits>

namespace NES::Configurations {

template<class T>
requires std::is_enum<T>::value class EnumOption : public TypedBaseOption<T> {
  public:
    EnumOption(std::string name, T value, std::string description);
    EnumOption<T>& operator=(const T& value);

  protected:
    void parseFromYAMLNode(Yaml::Node node) override;
    void parseFromString(std::string identifier, std::string value) override;
};

template<class T>
requires std::is_enum<T>::value EnumOption<T>::EnumOption(std::string name, T value, std::string description)
    : TypedBaseOption<T>(name, value, value, description){};

template<class T>
requires std::is_enum<T>::value EnumOption<T>
&EnumOption<T>::operator=(const T& value) {
    this->value = value;
    return *this;
}

template<class T>
requires std::is_enum<T>::value void EnumOption<T>::parseFromYAMLNode(Yaml::Node node) {
    parseFromString("", node.As<std::string>());
}

template<class T>
requires std::is_enum<T>::value void EnumOption<T>::parseFromString(std::string, std::string value) {
    if (!magic_enum::enum_contains<T>(value)) {
        auto name = std::string(magic_enum::enum_names_to_string<T>());
        throw ConfigurationException("Enum for " + value + " was not found. Valid options are " + name);
    }
    this->value = magic_enum::enum_cast<T>(value).value();
}

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_ENUMOPTION_HPP_
