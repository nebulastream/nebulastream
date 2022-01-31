#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_SEQUENCEOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_SEQUENCEOPTION_HPP_

#include <Configurations/ConfigOptions/BaseOption.hpp>
#include <vector>
namespace NES::Configurations {

template<class T>
requires std::is_base_of_v<BaseOption, T>
class SequenceOption : public BaseOption {
  public:
    SequenceOption(std::string name, std::string description);
    void clear() override;
    T operator[](size_t index) const;
    [[nodiscard]] size_t size() const;

  protected:
    void parseFromYAMLNode(Yaml::Node node) override;
    void parseFromString(std::string) override;

  private:
    std::vector<T> options;
};

template<class T>
requires std::is_base_of_v<BaseOption, T> SequenceOption<T>::SequenceOption(std::string name, std::string description)
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
    }
}
template<class T>
requires std::is_base_of_v<BaseOption, T>
void SequenceOption<T>::parseFromString(std::string) {}

template<class T>
requires std::is_base_of_v<BaseOption, T> T SequenceOption<T>::operator[](size_t index) const { return options[index]; }

template<class T>
requires std::is_base_of_v<BaseOption, T> size_t SequenceOption<T>::size()
const { return options.size(); }

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_SEQUENCEOPTION_HPP_
