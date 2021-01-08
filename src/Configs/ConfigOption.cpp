//
// Created by eleicha on 08.01.21.
//

#include <Configs/ConfigOption.hpp>
#include <cstring>
#include <sstream>
#include <string>

namespace NES {

template<typename Tp>
ConfigOption<Tp>::ConfigOption(std::string key, Tp value, std::string description, std::string dataType,
                               ConfigType usedConfiguration, bool isList)
    : key(key), value(value), description(description), dataType(dataType), usedConfiguration(usedConfiguration), isList(isList) {
}

template<typename Tp>
std::string ConfigOption<Tp>::toString() {
    std::stringstream ss;
    ss << "Config Object: \n";
    ss << "Name (key): " << key << "\n";
    ss << "Description: " << description << "\n";
    ss << "Value: " << value << "\n";
    ss << "Data Type: " << dataType << "\n";
    ss << "Configurations loaded from: " << configTypeToString(usedConfiguration) << "\n";
    ss << "Is Value a List? " << isList << ".";

    return ss.str();
}
template<typename Tp>
bool ConfigOption<Tp>::equals(std::any o) {
    if (this == o) {
        return true;
    } else if (o.has_value() && o.type() == typeid(ConfigOption)) {
        ConfigOption<Tp> that = (ConfigOption<Tp>) o;
        return this->key == that.key && this->description == that.description && this->value == that.value
            && this->dataType == that.dataType && this->usedConfiguration == that.usedConfiguration
            && this->isList == that.isList;
    }
    return false;
}

template<typename Tp>
ConfigOption<Tp>::~ConfigOption() {}

template<typename Tp>
std::string ConfigOption<Tp>::getKey() {
    return key;
}
template<typename Tp>
Tp ConfigOption<Tp>::getValue() {
    return value;
}
template<typename Tp>
std::string ConfigOption<Tp>::getDataType() {
    return dataType;
}
template<typename Tp>
bool ConfigOption<Tp>::getIsList() {
    return isList;
}
template<typename Tp>
ConfigType ConfigOption<Tp>::getUsedConfiguration() {
    return usedConfiguration;
}

template<typename Tp>
void ConfigOption<Tp>::setKey(std::string key, std::string description) {
    this->key = key;
    this->description = description;
}
template<typename Tp>
void ConfigOption<Tp>::setValue(Tp value, std::string dataType, ConfigType usedConfiguration, bool isList) {
    this->value = value;
    this->dataType = dataType;
    this->usedConfiguration = usedConfiguration;
    this->isList = isList;
}
template<typename Tp>
std::string ConfigOption<Tp>::configTypeToString(ConfigType configType) {
    {
        switch (configType) {
            case ConfigType::DEFAULT: return "default values";
            case ConfigType::YAML: return "YAML file values";
            case ConfigType::CONSOLE: return "console input values";
        }
    }
}

}// namespace NES