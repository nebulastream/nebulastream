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
#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_SEQUENCEOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_SEQUENCEOPTION_HPP_

#include <Configurations/ConfigOptions/BaseOption.hpp>
#include <Exceptions/ConfigurationException.hpp>
#include <vector>
namespace NES::Configurations {

/**
 * @brief This class implements sequential options of a type that has to be a subtype of the BaseOption.
 * @note SequenceOptions can't have default values.
 * @tparam Type of the component.
 */
template<class T>
requires std::is_base_of_v<BaseOption, T>
class SequenceOption : public BaseOption {
  public:
    /**
     * @brief Constructor to create a new option that sets a name, and description.
     * @param name of the option.
     * @param description of the option.
     */
    SequenceOption(const std::string& name, const std::string& description);

    /**
     * @brief Clears the option and removes all values in the sequence.
     */
    void clear() override;

    /**
     * @brief Accesses an option at a specific index.
     * @param index
     * @return Option of type T.
     */
    T operator[](size_t index) const;

    /**
     * @brief Returns the number of options.
     * @return size_t
     */
    [[nodiscard]] size_t size() const;

    [[nodiscard]] std::vector<T> getValues() const;
    [[nodiscard]] bool empty() const;
    template<class X>
    void add(X value) {
        auto option = T();
        option.setValue(value);
        options.push_back(option);
    };

  protected:
    void parseFromYAMLNode(Yaml::Node node) override;
    void parseFromString(std::string identifier, std::map<std::string, std::string>& inputParams) override;

  private:
    std::vector<T> options;
};

template<class T>
requires std::is_base_of_v<BaseOption, T> SequenceOption<T>::SequenceOption(const std::string& name,
                                                                            const std::string& description)
    : BaseOption(name, description){};

template<class T>
requires std::is_base_of_v<BaseOption, T>
void SequenceOption<T>::clear() { options.clear(); }

template<class T>
requires std::is_base_of_v<BaseOption, T>
void SequenceOption<T>::parseFromYAMLNode(Yaml::Node node) {
    if (node.IsSequence()) {
        for (auto child = node.Begin(); child != node.End(); child++) {
            auto identifier = (*child).first;
            auto value = (*child).second;
            auto option = T();
            option.parseFromYAMLNode(value);
            options.push_back(option);
        }
    } else {
        throw ConfigurationException("YAML node should be a sequence but it was a " + node.As<std::string>());
    }
}
template<class T>
requires std::is_base_of_v<BaseOption, T>
void SequenceOption<T>::parseFromString(std::string, std::map<std::string, std::string>&) {
    throw ConfigurationException("Currently we cant set sequential option with command line parameters");
}

template<class T>
requires std::is_base_of_v<BaseOption, T> T SequenceOption<T>::operator[](size_t index) const { return options[index]; }

template<class T>
requires std::is_base_of_v<BaseOption, T> size_t SequenceOption<T>::size()
const { return options.size(); }

template<class T>
requires std::is_base_of_v<BaseOption, T> std::vector<T> SequenceOption<T>::getValues()
const { return options; }

template<class T>
requires std::is_base_of_v<BaseOption, T>
bool SequenceOption<T>::empty() const { return options.empty(); }

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_SEQUENCEOPTION_HPP_
