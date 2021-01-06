//
// Created by eleicha on 06.01.21.
//

#ifndef NES_CONFIGOPTION_HPP
#define NES_CONFIGOPTION_HPP

#include <any>
#include <typeinfo>
#include <string>

namespace NES {

/**
 * @brief enum defining the origin of the configuration
 */
enum class ConfigType {
    DEFAULT,//default value
    YAML,   //obtained from the yaml config file
    CONSOLE //obtained from the console
};

template<typename _Tp>
class ConfigOption {
  public:
    ConfigOption(std::string key, _Tp value, std::string description, std::string dataType, ConfigType usedConfiguration,
                 bool isList);
    ~ConfigOption();

    /**
     * @brief converts a ConfigOption Object into human readable format
     */
    std::string toString();

    /**
     * @brief a method to make an object comparable
     */
     bool equals(std::any o);

    /**
      * @brief get the name of the ConfigOption Object
      */
    std::string getKey();

    /**
      * @brief get the value of the ConfigOption Object
      */
    _Tp getValue();

    /**
      * @brief obtain the enum value describing where the current value was obtained from (DEFAULT, YAML, CONSOLE)
      */
    ConfigType getConfigType();

    /**
       * @brief returns false if the value is just a value, and true if value consists of multiple values
       */
    bool getIsList();

    /**
   * @brief set the name of the ConfigOption Object and its associated value description
   */
    void setKey(std::string key, std::string description);

    /**
     * @brief set the value of the ConfigOption Object as well as the values associated with it,
     * i.e. dataType, usedConfiguration, isList
     */
    void setValue(_Tp value, std::string dataType, ConfigType usedConfiguration, bool isList);

  private:
    ConfigOption();

    std::string key;
    std::string description;
    _Tp value;
    std::string dataType;
    ConfigType usedConfiguration;
    bool isList;
};

}// namespace NES

#endif//NES_CONFIGOPTION_HPP
