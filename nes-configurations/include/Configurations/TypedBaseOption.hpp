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
#pragma once
#include <memory>
#include <typeinfo>
#include <utility>
#include <vector>
#include <Configurations/BaseOption.hpp>
#include <Configurations/ReadingVisitor.hpp>
#include <Configurations/Validation/ConfigurationValidation.hpp>
#include <Configurations/WritingVisitor.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::Configurations
{


/// This class is a base class, which represents an option that holds values of a particular type T.
template <class T>
class TypedBaseOption : public BaseOption
{
public:
    /// Constructor to create a new option without initializing any members.
    /// This is required to create nested options, e.g., a IntOption  that is part of a sequence.
    TypedBaseOption();

    TypedBaseOption(const std::string& name, const std::string& description);

    TypedBaseOption(const std::string& name, T defaultValue, const std::string& description);

    TypedBaseOption(
        const std::string& name,
        T defaultValue,
        const std::string& description,
        std::vector<std::shared_ptr<ConfigurationValidation>> validators);

    /// Operator to directly access the value of this option.
    operator T() const { return this->value; }

    /// Clears the option and sets the value to the default value.
    void clear() override;

    [[nodiscard]] T getValue() const;

    void setValue(T newValue);

    [[nodiscard]] const T& getDefaultValue() const;

protected:
    T value;
    T defaultValue;
    std::vector<std::shared_ptr<ConfigurationValidation>> validators;

    /// Iterates over all validators of this option before setting a new value. pValue is the value to be tested for validity.
    void isValid(std::string);

public:
    void accept(ReadingVisitor& visitor) override { visitor.visit(*this); }

    void accept(WritingVisitor& visitor) override { visitor.visit(*this); }
};

template <class T>
TypedBaseOption<T>::TypedBaseOption() : BaseOption()
{
}

template <class T>
TypedBaseOption<T>::TypedBaseOption(const std::string& name, const std::string& description) : BaseOption(name, description)
{
}

template <class T>
TypedBaseOption<T>::TypedBaseOption(const std::string& name, T defaultValue, const std::string& description)
    : BaseOption(name, description), defaultValue(defaultValue)
{
    /// With clear() we avoid the case value == null, ProtobufDeserialize only works with value != null
    TypedBaseOption<T>::clear();
}

template <class T>
TypedBaseOption<T>::TypedBaseOption(
    const std::string& name,
    T defaultValue,
    const std::string& description,
    std::vector<std::shared_ptr<ConfigurationValidation>> validators)
    : BaseOption(name, description), defaultValue(defaultValue), validators(std::move(validators))
{
    TypedBaseOption<T>::clear();
}

template <class T>
T TypedBaseOption<T>::getValue() const
{
    return value;
};

template <class T>
void TypedBaseOption<T>::setValue(T newValue)
{
    this->value = newValue;
}

template <class T>
const T& TypedBaseOption<T>::getDefaultValue() const
{
    return defaultValue;
}
template <class T>
void TypedBaseOption<T>::clear()
{
    this->value = defaultValue;
}

template <class T>
void TypedBaseOption<T>::isValid(std::string pValue)
{
    bool invalid = false;
    std::map<std::string, std::string> failureMessages;
    if (this->validators.empty())
    {
        return;
    }
    for (auto validator : this->validators)
    {
        if (!(validator->isValid(pValue)))
        {
            std::string validatorName;
            std::string message;
            validatorName = typeid(validator).name();
            message = "Validator (" + validatorName + ") failed for " + this->name + " with value: " + pValue;
            failureMessages[validatorName] = message;
        }
    }
    if (!failureMessages.empty())
    {
        std::string exceptionMessage;
        for (auto pair : failureMessages)
        {
            exceptionMessage += pair.second + "\n";
        }
        throw InvalidConfigParameter(exceptionMessage);
    }
}

}
