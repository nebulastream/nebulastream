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

#include <ostream>
#include <Configurations/TypedBaseOption.hpp>
#include <Configurations/Validation/ConfigurationValidation.hpp>
#include <Util/Logger/Logger.hpp>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>

namespace NES::Configurations
{

/// @brief This class provides a general implementation for all ScalarOption<T> this is used e.g., for IntOption, StringOption, BoolOption.
template <class T>
class ScalarOption : public TypedBaseOption<T>
{
public:
    ScalarOption(const std::string& name, const std::string& description);
    ScalarOption(const std::string& name, const std::string& defaultValue, const std::string& description);
    ScalarOption(
        const std::string& name,
        const std::string& defaultValue,
        const std::string& description,
        std::vector<std::shared_ptr<ConfigurationValidation>> validators);

    ScalarOption<T>& operator=(const T& value);

    bool operator==(const BaseOption& other) override;
    bool operator==(const T& other);

    /// Operator to directly access the value of this option.
    operator T() { return this->value; }

    template <class X>
    friend std::ostream& operator<<(std::ostream& os, const ScalarOption<X>& option);
    std::string toString() override;

protected:
    virtual void parseFromYAMLNode(YAML::Node node) override;
    void parseFromString(std::string identifier, std::unordered_map<std::string, std::string>& inputParams) override;

private:
    template <DerivedBaseOption X>
    friend class SequenceOption;

    /// @brief Private constructor to create a scalar option without a name and description.
    /// This can only be used in SequenceOptions
    ScalarOption() : TypedBaseOption<T>() { }

    template <typename Type>
    static Type convertFromString(const std::string& strValue)
    {
        if constexpr (std::is_same<Type, std::string>::value)
        {
            return strValue; /// No conversion needed
        }
        else if constexpr (std::is_same<Type, float>::value)
        {
            return std::stof(strValue);
        }
        else if constexpr (std::is_same<Type, uint64_t>::value)
        {
            return std::stoull(strValue);
        }
        else if constexpr (std::is_same<Type, int64_t>::value)
        {
            return std::stoull(strValue);
        }
        else if constexpr (std::is_same<Type, bool>::value)
        {
            /// Simple boolean conversion (true for "true", false otherwise)
            return strValue == "true";
        }
        else if constexpr (NESIdentifier<Type>)
        {
            return Type(convertFromString<typename Type::Underlying>(strValue));
        }
        else
        {
            throw std::logic_error("Unsupported type for ScalarOption");
        }
    }
};

template <class T>
std::string ScalarOption<T>::toString()
{
    std::stringstream os;
    os << "Name: " << this->name << "\n";
    os << "Description: " << this->description << "\n";
    os << "Value: " << this->value << "\n";
    os << "Default Value: " << this->defaultValue << "\n";
    return os.str();
}

template <class X>
std::ostream& operator<<(std::ostream& os, const ScalarOption<X>& option)
{
    os << "Name: " << option.name << "\n";
    os << "Description: " << option.description << "\n";
    os << "Value: " << option.value << "\n";
    os << "Default Value: " << option.defaultValue << "\n";
    return os;
}

template <class T>
ScalarOption<T>::ScalarOption(const std::string& name, const std::string& description) : TypedBaseOption<T>(name, description)
{
}

template <class T>
ScalarOption<T>::ScalarOption(const std::string& name, const std::string& value, const std::string& description)
    : TypedBaseOption<T>(name, convertFromString<T>(value), description)
{
}

template <class T>
ScalarOption<T>::ScalarOption(
    const std::string& name,
    const std::string& value,
    const std::string& description,
    std::vector<std::shared_ptr<ConfigurationValidation>> validators)
    : TypedBaseOption<T>(name, convertFromString<T>(value), description, validators)
{
    this->validators = validators;
    this->isValid(value);
}

template <class T>
ScalarOption<T>& ScalarOption<T>::operator=(const T& value)
{
    this->value = value;
    return *this;
}

template <class T>
bool ScalarOption<T>::operator==(const BaseOption& other)
{
    return TypedBaseOption<T>::operator==(other);
}

template <class T>
bool ScalarOption<T>::operator==(const T& other)
{
    return this->value == other;
}

template <class T>
void ScalarOption<T>::parseFromYAMLNode(YAML::Node node)
{
    this->isValid(node.as<std::string>());
    this->value = node.as<T>();
}

template <class T>
void ScalarOption<T>::parseFromString(std::string identifier, std::unordered_map<std::string, std::string>& inputParams)
{
    if (!inputParams.contains(this->getName()))
    {
        throw InvalidConfigParameter("Identifier {} is not known.", identifier);
    }
    std::string value = inputParams[this->getName()];
    if (value.empty())
    {
        throw InvalidConfigParameter("Identifier {} is not known.", identifier);
    }
    this->isValid(value);
    try
    {
        this->value = YAML::Load(value).as<T>();
    }
    catch (const std::exception& e)
    {
        throw InvalidConfigParameter("Conversion failed for {} with value: {}. Exception: {}", identifier, value, e.what());
    }
}

using StringOption = ScalarOption<std::string>;
using FloatOption = ScalarOption<float>;
using UIntOption = ScalarOption<uint64_t>;
using BoolOption = ScalarOption<bool>;

}
