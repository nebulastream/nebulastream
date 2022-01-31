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
    void parseFromString(std::string basicString) override;
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
    parseFromString(node.As<std::string>());
}

template<class T>
requires std::is_enum<T>::value void EnumOption<T>::parseFromString(std::string basicString) {
    if (!magic_enum::enum_contains<T>(basicString)) {
        throw ConfigurationException("Enum for " + basicString + " was not found");
    }
    this->value = magic_enum::enum_cast<T>(basicString).value();
}

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_ENUMOPTION_HPP_
