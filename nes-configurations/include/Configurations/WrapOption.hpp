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

#include <string>
#include <unordered_map>
#include <Configurations/TypedBaseOption.hpp>

namespace NES::Configurations
{

template <class Type, class Factory>
concept IsFactory = requires(std::string identifier, std::unordered_map<std::string, std::string>& inputParams, YAML::Node node) {
    { Factory::createFromString(identifier, inputParams) };
    { Factory::createFromYaml(node) };
};

/**
 * @brief This class provides a general option, that can wrap an object of arbitrary Type as an option.
 * To this end, users have to specific a Factory which implements the following two static functions:
 * - Type Factory::createFromString(std::string) -> creates an object of the option Type from an string value.
 * - Type Factory::createFromYaml(Yaml::Node) -> creates an object of the option Type from an YAML node.
 * This allows users to represent custom domain types as members of a configuration object, e.g., see PhysicalSources in the WorkerConfiguration.
 * @note WrapOptions can't have a default type.
 * @tparam Type of the object that is wrapped by the option.
 * @tparam Factory type which implements the static create function to initialize a value of this option.
 */
template <class Type, class Factory>
requires IsFactory<Type, Factory>
class WrapOption : public TypedBaseOption<Type>
{
public:
    /**
     * @brief Constructor to create a new option that sets a name, and description.
     * @param name of the option.
     * @param description of the option.
     */
    WrapOption(const std::string& name, const std::string& description);
    std::string toString() override;

protected:
    virtual void parseFromYAMLNode(YAML::Node node) override;
    void parseFromString(std::string identifier, std::unordered_map<std::string, std::string>& inputParams) override;

private:
    template <DerivedBaseOption X>
    friend class SequenceOption;

    WrapOption() : TypedBaseOption<Type>() { }
};

template <class Type, class Factory>
requires IsFactory<Type, Factory>
WrapOption<Type, Factory>::WrapOption(const std::string& name, const std::string& description) : TypedBaseOption<Type>(name, description)
{
}

template <class Type, class Factory>
requires IsFactory<Type, Factory>
void WrapOption<Type, Factory>::parseFromString(std::string identifier, std::unordered_map<std::string, std::string>& inputParams)
{
    this->value = Factory::createFromString(identifier, inputParams);
}

template <class Type, class Factory>
requires IsFactory<Type, Factory>
void WrapOption<Type, Factory>::parseFromYAMLNode(YAML::Node node)
{
    this->value = Factory::createFromYaml(node);
}

template <class Type, class Factory>
requires IsFactory<Type, Factory>
std::string Configurations::WrapOption<Type, Factory>::toString()
{
    return "";
}

}
