#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGURATION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGURATION_HPP_
#include <Configurations/ConfigOptions/BaseOption.hpp>
#include <Configurations/ConfigOptions/EnumOption.hpp>
#include <Configurations/ConfigOptions/ScalarOption.hpp>
#include <Configurations/ConfigOptions/SequenceOption.hpp>
#include <Configurations/ConfigOptions/WrapOption.hpp>
#include <Exceptions/ConfigurationException.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <string>
namespace NES::Configurations {

class BaseConfiguration : public BaseOption {
  public:
    BaseConfiguration();
    BaseConfiguration(std::string name, std::string description);
    virtual ~BaseConfiguration() = default;
    void parseFromYAMLNode(Yaml::Node config) override;
    void parseFromString(std::string) override;
    /**
     * @brief overwrite the default configurations with those loaded from a yaml file
     * @param filePath file path to the yaml file
     */
    void overwriteConfigWithYAMLFileInput(const std::string& filePath);

    /**
     * @brief overwrite the default and the yaml file configurations with command line input
     * @param inputParams map with key=command line parameter and value = value
     */
    void overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams);

    /**
     * @brief clears all options and set the default values
     */
    void clear() override;

  protected:
    virtual std::vector<Configurations::BaseOption*> getOptions() = 0;
    std::map<std::string, Configurations::BaseOption*> getOptionMap();
};

}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGURATION_HPP_
