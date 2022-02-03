#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_WRAPOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_WRAPOPTION_HPP_

#include <Configurations/ConfigOptions/TypedBaseOption.hpp>

namespace NES::Configurations {

template<class T>
concept IsFactory = requires(std::string z, Yaml::Node node) {
    T::createFromString(z);
    T::createFromYaml(node);
};

template<class Type, IsFactory Factory>
class WrapOption : public TypedBaseOption<Type> {
  public:
    WrapOption(std::string name, std::string description);
    virtual void parseFromYAMLNode(Yaml::Node node) override;
    void parseFromString(std::string basicString) override;

  private:
    template<class X>
    requires std::is_base_of_v<BaseOption, X>
    friend class SequenceOption;
    WrapOption() : TypedBaseOption<Type>() {}
};

template<class Type, IsFactory Factory>
WrapOption<Type, Factory>::WrapOption(std::string name, std::string description) : TypedBaseOption<Type>(name, description) {}

template<class Type, IsFactory Factory>
void WrapOption<Type, Factory>::parseFromString(std::string basicString) {
    Factory::createFromString(basicString);
}

template<class Type, IsFactory Factory>
void WrapOption<Type, Factory>::parseFromYAMLNode(Yaml::Node node) {
    Factory::createFromYaml(node);
}

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_WRAPOPTION_HPP_
