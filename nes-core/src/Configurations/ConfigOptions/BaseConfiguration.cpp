#include <Configurations/ConfigOptions/BaseConfiguration.hpp>

namespace NES::Configurations {

BaseConfiguration::BaseConfiguration() : BaseOption(){};

BaseConfiguration::BaseConfiguration(const std::string& name, const std::string& description) : BaseOption(name, description){};

void BaseConfiguration::parseFromYAMLNode(const Yaml::Node config) {
    auto optionMap = getOptionMap();
    if (!config.IsMap()) {
        throw ConfigurationException("Malformed YAML configuration file");
    }
    for (auto entry = config.Begin(); entry != config.End(); entry++) {
        auto identifier = (*entry).first;
        auto node = (*entry).second;
        if (!optionMap.contains(identifier)) {
            throw ConfigurationException("Identifier: " + identifier + " is not known.");
        }
        // check if config is empty
        if (!config.As<std::string>().empty() && config.As<std::string>() != "\n") {
            throw ConfigurationException("Value for " + identifier + " is empty.");
        }
        optionMap[identifier]->parseFromYAMLNode(node);
    }
}

void BaseConfiguration::parseFromString(const std::string& identifier, const std::string& value) {
    auto optionMap = getOptionMap();
    if (value.empty()) {
        throw ConfigurationException("The value field in empty.");
    }

    if (identifier.find('.') != std::string::npos) {
        // identifier contains a dot so it refers an nested value.
        // get first identifier
        auto index = std::string(identifier).find('.');
        auto parentIdentifier = std::string(identifier).substr(0, index);
        if (!optionMap.contains(parentIdentifier)) {
            throw ConfigurationException("Identifier " + parentIdentifier + " is not known.");
        }
        auto childrenIdentifier = std::string(identifier).substr(index + 1, identifier.length());
        optionMap[parentIdentifier]->parseFromString(childrenIdentifier, value);
    } else {
        if (!optionMap.contains(identifier)) {
            throw ConfigurationException("Identifier " + identifier + " is not known.");
        }
        optionMap[identifier]->parseFromString(identifier, value);
    }
}

void BaseConfiguration::overwriteConfigWithYAMLFileInput(const std::string& filePath) {
    Yaml::Node config;
    Yaml::Parse(config, filePath.c_str());
    if (config.IsNone()) {
        return;
    }
    parseFromYAMLNode(config);
}

void BaseConfiguration::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    for (auto parm = inputParams.begin(); parm != inputParams.end(); ++parm) {
        auto identifier = parm->first;
        auto value = parm->second;
        if (!identifier.starts_with("--")) {
            throw ConfigurationException("Identifier " + identifier + " is not malformed. All commands should start with a --.");
        }
        // remove the -- in the beginning
        identifier = identifier.substr(2);
        parseFromString(identifier, value);
    }
}

void BaseConfiguration::clear() {
    for (auto* option : getOptions()) {
        option->clear();
    }
};

std::map<std::string, Configurations::BaseOption*> BaseConfiguration::getOptionMap() {
    std::map<std::string, Configurations::BaseOption*> optionMap;
    for (auto* option : getOptions()) {
        auto identifier = option->getName();
        optionMap[identifier] = option;
    }
    return optionMap;
}

}// namespace NES::Configurations