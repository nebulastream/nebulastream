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
#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_TYPEDBASEOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_TYPEDBASEOPTION_HPP_
#include <Configurations/ConfigOptions/BaseOption.hpp>
namespace NES::Configurations {

/**
 * @brief This class is a base class, which represents an option that holds values of a particular type T.
 * @tparam T of the value.
 */
template<class T>
class TypedBaseOption : public BaseOption {
  public:

    /**
     * @brief Constructor to create a new option without initializing any members.
     * This is required to create nested options, e.g., a IntOption  that is part of a sequence.
     */
    TypedBaseOption();

    /**
     * @brief Constructor to create a new option that sets a name, and description.
     * @param name of the option.
     * @param description of the option.
     */
    TypedBaseOption(const std::string& name, const std::string& description);

    /**
     * @brief Constructor to create a new option that declares a specific default value.
     * @param name of the option.
     * @param defaultValue of the option. Has to be of type T.
     * @param description of the option.
     */
    TypedBaseOption(const std::string& name, T defaultValue, const std::string& description);

    /**
     * @brief Operator to directly access the value of this option.
     * @return Returns an object of the option type T.
     */
    operator T() const { return this->value; }

    /**
     * @brief Clears the option and sets the value to the default value.
     */
    void clear() override;

    /**
     * @brief get the value of the ConfigurationOption Object
     * @return the value of the config if not set then default value
    */
    [[nodiscard]] T getValue() const;

    /**
    * @brief Sets the value
    * @param newValue the new value to be used
    */
    void setValue(T newValue);

    /**
     * @brief Getter to access the default value of this option.
     * @return default value
     */
    [[nodiscard]] const T& getDefaultValue() const;

  protected:
    T value;
    T defaultValue;
};

template<class T>
TypedBaseOption<T>::TypedBaseOption() : BaseOption() {}

template<class T>
TypedBaseOption<T>::TypedBaseOption(const std::string& name, const std::string& description) : BaseOption(name, description) {}

template<class T>
TypedBaseOption<T>::TypedBaseOption(const std::string& name, T defaultValue, const std::string& description)
    : BaseOption(name, description), value(defaultValue), defaultValue(defaultValue) {}

template<class T>
T TypedBaseOption<T>::getValue() const {
    return value;
};

template<class T>
void TypedBaseOption<T>::setValue(T newValue) {
    this->value = newValue;
}

template<class T>
const T& TypedBaseOption<T>::getDefaultValue() const {
    return defaultValue;
}
template<class T>
void TypedBaseOption<T>::clear() {
    this->value = defaultValue;
}

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_TYPEDBASEOPTION_HPP_
