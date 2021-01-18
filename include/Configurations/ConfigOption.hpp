/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_CONFIGOPTION_HPP
#define NES_CONFIGOPTION_HPP

#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <any>
#include <sstream>
#include <string>
#include <typeinfo>

using namespace std;

namespace NES {
/**
 * @brief Template for a ConfigOption object
 * @tparam T template parameter, depends on ConfigOptions
 */
template<class T>
class ConfigOption {
  public:
    /**
     * @brief Constructs a ConfigOption<T> object
     * @param key the name of the object
     * @param value the value of the object
     * @param description default value of the object
     * @param dataType data type of the object
     * @param isList boolean indicating a list
     */
    ConfigOption(std::string key, T value, string description, string dataType, bool isList);

    /**
     * @brief converts a ConfigOption Object into human readable format
     */
    std::string toString();

    /**
     * @brief converts the value of this object into a string
     * @return string of the value of this object
     */
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

    /**
     * @brief returns the data type of the value of this object
     */
    string getDataType();

    /**
     * @brief sets the value
     */
    void setValue(T value);

    /**
     * @brief get the description of this parameter
     */
    const string& getDescription() const;

    /**
     * @brief get the default value of this parameter
     */
    T getDefaultValue() const;

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
    : key(key), value(value), defaultValue(value), description(description), dataType(dataType), isList(isList) {}

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

}// namespace NES

#endif//NES_CONFIGOPTION_HPP
