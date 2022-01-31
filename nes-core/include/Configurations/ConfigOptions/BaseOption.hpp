#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_BASEOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_BASEOPTION_HPP_
#include <Util/yaml/Yaml.hpp>
#include <string>
namespace NES::Configurations {
class BaseOption {
  public:
    BaseOption() = default;
    BaseOption(std::string name, std::string description);
    virtual ~BaseOption() = default;
    virtual void clear() = 0;
    virtual bool operator==(const BaseOption& other);
    std::string getName();
    std::string getDescription();

  protected:
    friend class BaseConfiguration;
    virtual void parseFromYAMLNode(Yaml::Node node) = 0;
    virtual void parseFromString(std::string basicString) = 0;
    std::string name;
    std::string description;
};
}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_BASEOPTION_HPP_
