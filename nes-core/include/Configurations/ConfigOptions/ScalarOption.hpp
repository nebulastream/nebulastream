#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_SCALAROPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_SCALAROPTION_HPP_

#include <Configurations/ConfigOptions/TypedBaseOption.hpp>
namespace NES::Configurations {

/**
 * @brief Template for a ConfigurationOption object
 * @tparam T template parameter, depends on ConfigOptions
 */
template<class T>
class ScalarOption : public TypedBaseOption<T> {
  public:
    ScalarOption(std::string name, T value, std::string description);
    ScalarOption(std::string name, T value, T defaultValue, std::string description);
    ScalarOption<T>& operator=(const T& value);
    void clear() override;
    bool operator==(const BaseOption& other) override;

  protected:
    virtual void parseFromYAMLNode(Yaml::Node node) override { this->value = node.As<T>(); }
    void parseFromString(std::string basicString) override { this->value = Yaml::impl::StringConverter<T>::Get(basicString); }

  private:
    template<class X>
    requires std::is_base_of_v<BaseOption, X>
    friend class SequenceOption;
    ScalarOption() : TypedBaseOption<T>() {}
};

template<class T>
ScalarOption<T>::ScalarOption(std::string name, T value, std::string description)
    : TypedBaseOption<T>(name, value, description) {}

template<class T>
ScalarOption<T>::ScalarOption(std::string name, T value, T defaultValue, std::string description)
    : TypedBaseOption<T>(name, value, defaultValue, description) {}

template<class T>
ScalarOption<T>& ScalarOption<T>::operator=(const T& value) {
    this->value = value;
    return *this;
}

template<class T>
void ScalarOption<T>::clear() {
    this->value = this->defaultValue;
}

template<class T>
bool ScalarOption<T>::operator==(const BaseOption& other) {
    return TypedBaseOption<T>::operator==(other);
}

using StringOption = ScalarOption<std::string>;
using IntOption = ScalarOption<int64_t>;
using UIntOption = ScalarOption<uint64_t>;
using BoolOption = ScalarOption<bool>;

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_SCALAROPTION_HPP_
