#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_WRAPOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_WRAPOPTION_HPP_

#include <Configurations/ConfigOptions/TypedBaseOption.hpp>

namespace NES::Configurations {

template<class Type, class Factory>
concept IsFactory = requires(std::string z, Yaml::Node node) {
    {Factory::createFromString(z)} -> std::convertible_to<Type>;
    {Factory::createFromYaml(node)} -> std::convertible_to<Type>;
};

template<class Type, class Factory>
requires IsFactory<Type, Factory>
class WrapOption : public TypedBaseOption<Type> {
  public:
    WrapOption(std::string name, std::string description);
    virtual void parseFromYAMLNode(Yaml::Node node) override;
    void parseFromString(std::string identifier, std::string basicString) override;
  private:
    template<class X>
    requires std::is_base_of_v<BaseOption, X>
    friend class SequenceOption;
    WrapOption() : TypedBaseOption<Type>() {}
};

template<class Type, class Factory>
requires IsFactory<Type, Factory>
WrapOption<Type, Factory>::WrapOption(std::string name, std::string description) : TypedBaseOption<Type>(name, description) {}

template<class Type, class Factory>
requires IsFactory<Type, Factory>
void WrapOption<Type, Factory>::parseFromString(std::string, std::string value) {
    this->value = Factory::createFromString(value);
}

template<class Type,class Factory>
requires IsFactory<Type, Factory>
void WrapOption<Type, Factory>::parseFromYAMLNode(Yaml::Node node) {
    this->value = Factory::createFromYaml(node);
}

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_WRAPOPTION_HPP_
