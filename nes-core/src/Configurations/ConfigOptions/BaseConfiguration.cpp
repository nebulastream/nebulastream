#include <Configurations/ConfigOptions/BaseConfiguration.hpp>

namespace NES::Configurations {

BaseConfiguration::BaseConfiguration() : BaseOption(){};

BaseConfiguration::BaseConfiguration(std::string name, std::string description) : BaseOption(name, description){};

void BaseConfiguration::parseFromYAMLNode(Yaml::Node config) {
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

void BaseConfiguration::parseFromString(std::string) {}

void BaseConfiguration::overwriteConfigWithYAMLFileInput(const std::string& filePath) {
    Yaml::Node config;
    Yaml::Parse(config, filePath.c_str());
    if (config.IsNone())
        return;
    parseFromYAMLNode(config);
}

void BaseConfiguration::overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams) {
    auto optionMap = getOptionMap();
    for (auto parm = inputParams.begin(); parm != inputParams.end(); ++parm) {
        auto identifier = parm->first;
        auto value = parm->second;
        if (identifier.starts_with("--")) {
            throw ConfigurationException("Identifier " + identifier + " is not malformed. All commands should start with a --.");
        }
        // remove the -- in the beginning
        identifier = identifier.substr(2);

        if (!optionMap.contains(identifier)) {
            throw ConfigurationException("Identifier " + identifier + " is not known.");
        }

        if (value.empty()) {
            throw ConfigurationException("The value field in empty.");
        }

        optionMap[identifier]->parseFromString(value);
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
        optionMap[option->getName()] = option;
    }
    return optionMap;
}

}// namespace NES::Configurations