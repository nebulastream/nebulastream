//
// Created by eleicha on 06.01.21.
//

#ifndef NES_CONFIGOPTION_HPP
#define NES_CONFIGOPTION_HPP

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

    uint16_t getValueAsUint16_t();

    uint32_t getValueAsUint32_t();

    uint64_t getValueAsUint64_t();

    bool getValueAsBool();

    string getValueAsString();

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
    T getValue();

    /**
       * @brief returns false if the value is just a value, and true if value consists of multiple values
       */
    bool getIsList();

    string getDataType();

    void setUint16_tValue(uint16_t value);
    void setUint32_tValue(uint32_t value);
    void setUint64_tValue(uint64_t value);
    void setBoolValue(bool value);
    void setStringValue(string value);


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
uint16_t ConfigOption<T>::getValueAsUint16_t() {

    if (dataType == "uint16_t" && !isList) {
        return uint16_t(value);
    } else {
        NES_ERROR("Value type not available in format uint16_t");
        return 0;
    }
}

template<typename T>
uint32_t ConfigOption<T>::getValueAsUint32_t() {

    if ((dataType == "uint16_t" || dataType == "uint32_t") && !isList) {
        return value;
    } else {
        NES_ERROR("Value type not available in format uint32_t");
        return 0;
    }
}

template<typename T>
uint64_t ConfigOption<T>::getValueAsUint64_t() {

    if ((dataType == "uint16_t" || dataType == "uint32_t" || dataType == "uint64_t") && !isList) {
        return value;
    } else {
        NES_ERROR("Value type not available in format uint64_t");
        return 0;
    }
}

template<typename T>
bool ConfigOption<T>::getValueAsBool() {
    if (dataType == "bool" && !isList) {
        return value;
    } else {
        NES_ERROR("Value type not available in format bool");
        return false;
    }
}
template<typename T>
string ConfigOption<T>::getValueAsString() {
    if (dataType == "string" && !isList) {
        return string(value);
    } else {
        return string(value);
    }
}

template<class T>
void ConfigOption<T>::setUint16_tValue(uint16_t value) {
    this->value = value;
    this->isList = false;
}
template<class T>
void ConfigOption<T>::setUint32_tValue(uint32_t value) {
    this->value = value;
    this->isList = false;
}
template<class T>
void ConfigOption<T>::setUint64_tValue(uint64_t value) {
    this->value = value;
    this->isList = false;
}
template<class T>
void ConfigOption<T>::setBoolValue(bool value) {
    this->value = value;
    this->isList = false;
}
template<class T>
void ConfigOption<T>::setStringValue(string value) {
    this->value = value;
    this->isList = false;
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
T ConfigOption<T>::getValue() {
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

}// namespace NES

#endif//NES_CONFIGOPTION_HPP
