#ifndef NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_BASEOPTION_HPP_
#define NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_BASEOPTION_HPP_
#include <Util/yaml/Yaml.hpp>
#include <string>
namespace NES::Configurations {

/**
 * @brief This class is the basis of all option.
 * All option can define a name and a description.
 */
class BaseOption {
  public:
    BaseOption() = default;
    /**
     * @brief Constructor to create a new option.
     * @param name of the option.
     * @param description of the option.
     */
    BaseOption(const std::string& name, const std::string& description);
    virtual ~BaseOption() = default;
    /**
     * @brief Clears the option and sets a default value if available.
     */
    virtual void clear() = 0;
    /**
     * @brief Checks if the option is equal to another option.
     * @param other option.
     * @return true if the option is equal.
     */
    virtual bool operator==(const BaseOption& other);

    /**
     * @brief Getter for the name of the option.
     * @return name
     */
    std::string getName();

    /**
     * @brief Getter for the description of the option.
     * @return description
     */
    std::string getDescription();

  protected:
    friend class BaseConfiguration;

    /**
     * @brief ParseFromYamlNode fills the content of this option with the value of the YAML node.
     * @param node
     */
    virtual void parseFromYAMLNode(Yaml::Node node) = 0;

    /**
     * @brief ParseFromString fills the content of this option with a specific string value.
     * If this option is nested it uses the identifier to lookup the particular children option.
     * @param identifier of the children option if the option is nested.
     * @param value of the option as a string
     */
    virtual void parseFromString(const std::string& identifier, const std::string& value) = 0;

    std::string name;
    std::string description;
};
}// namespace NES::Configurations

#endif//NES_NES_CORE_INCLUDE_CONFIGURATIONS_CONFIGOPTIONS_BASEOPTION_HPP_
