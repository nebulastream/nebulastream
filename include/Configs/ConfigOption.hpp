//
// Created by eleicha on 06.01.21.
//

#ifndef NES_CONFIGOPTION_HPP
#define NES_CONFIGOPTION_HPP

#include <Util/yaml/YamlDef.hh>
#include <Util/Logger.hpp>
#include <any>
#include <sstream>
#include <string>
#include <typeinfo>

using namespace std;

namespace NES {

template<class T>
class ConfigOption {
  public:
    ConfigOption(std::string key, T value, string description, string dataType, bool isList);

    /**
     * @brief converts a ConfigOption Object into human readable format
     */
    std::string toString();

    string getValueAsString() const;

    /**
     * @brief a method to make an object comparable
     */
    bool equals(any o);

    /**
      * @brief get the name of the ConfigOption Object
      */
    std::string getKey();

    /**
      * @brief get the value of the ConfigOption Object
      */
    T getValue() const;

    /**
       * @brief returns false if the value is just a value, and true if value consists of multiple values
       */
    bool getIsList();

    string getDataType();

    void setValue(T value);

    const string& getDescription() const;
    T getDefaultValue() const;
    bool isList1() const;

  private:
    std::string key;
    std::string description;
    T value;
    T defaultValue;
    string dataType;
    bool isList;
};

template<class T>
ConfigOption<T>::ConfigOption(std::string key, T value, string description, string dataType, bool isList)
    : key(key), value(value), defaultValue(value), description(description), dataType(dataType), isList(isList) {
}

template<typename T>
string ConfigOption<T>::getValueAsString() const {
    if (dataType == "string" && !isList) {
        return string(value);
    } else {
        return string(value);
    }
}

template<class T>
void ConfigOption<T>::setValue(T value) {
    this->value = value;
}

template<class T>
std::string ConfigOption<T>::toString() {
    std::stringstream ss;
    ss << "Config Object: \n";
    ss << "Name (key): " << key << "\n";
    ss << "Description: " << description << "\n";
    ss << "Value: " << value << "\n";
    ss << "Default Value: " << defaultValue << "\n";
    ss << "Data Type: " << dataType << "\n";
    ss << "Is Value a List? " << isList << ".";

    return ss.str();
}
template<class T>
bool ConfigOption<T>::equals(std::any o) {
    if (this == o) {
        return true;
    } else if (o.has_value() && o.type() == typeid(ConfigOption)) {
        ConfigOption<T> that = (ConfigOption<T>) o;
        return this->key == that.key && this->description == that.description && this->value == that.value
            && this->dataType == that.dataType && this->defaultValue == that.defaultValue && this->isList == that.isList;
    }
    return false;
}

template<class T>
std::string ConfigOption<T>::getKey() {
    return key;
}
template<class T>
T ConfigOption<T>::getValue() const {
    return value;
}
template<class T>
std::string ConfigOption<T>::getDataType() {
    return dataType;
}
template<class T>
bool ConfigOption<T>::getIsList() {
    return isList;
}
template<class T>
const string& ConfigOption<T>::getDescription() const {
    return description;
}
template<class T>
T ConfigOption<T>::getDefaultValue() const {
    return defaultValue;
}
template<class T>
bool ConfigOption<T>::isList1() const {
    return isList;
}

}// namespace NES

#endif//NES_CONFIGOPTION_HPP
