#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_TYPEDBASEOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_TYPEDBASEOPTION_HPP_
#include <Configurations/ConfigOptions/BaseOption.hpp>
namespace NES::Configurations {

template<class T>
class TypedBaseOption : public BaseOption {
  public:
    TypedBaseOption();
    TypedBaseOption(std::string name, T value, std::string description);
    TypedBaseOption(std::string name, T value, T defaultValue, std::string description);

    operator T() const { return this->value; }
    bool operator==(const BaseOption& other) override;
    [[nodiscard]] T getDefaultValue() const;
    void clear() override;

    /**
     * @brief get the value of the ConfigurationOption Object
     * @return the value of the config if not set then default value
    */
    [[nodiscard]] T getValue() const;

    /**
    * @brief sets the value
    * @param value: the value to be used
    */
    void setValue(T value);

  protected:
    T value;
    T defaultValue;
};

template<class T>
TypedBaseOption<T>::TypedBaseOption() : BaseOption() {}

template<class T>
TypedBaseOption<T>::TypedBaseOption(std::string name, T value, std::string description)
    : BaseOption(name, description), value(value), defaultValue(value) {}

template<class T>
TypedBaseOption<T>::TypedBaseOption(std::string name, T value, T defaultValue, std::string description)
    : BaseOption(name, description), value(value), defaultValue(defaultValue) {}

template<class T>
T TypedBaseOption<T>::getValue() const {
    return value;
};

template<class T>
void TypedBaseOption<T>::setValue(T value) {
    this->value = value;
}

template<class T>
T TypedBaseOption<T>::getDefaultValue() const {
    return defaultValue;
}
template<class T>
void TypedBaseOption<T>::clear() {
    this->value = defaultValue;
}

template<class T>
bool TypedBaseOption<T>::operator==(const BaseOption& other) {
    return this->BaseOption::operator==(other) && value == value;
}

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_TYPEDBASEOPTION_HPP_
