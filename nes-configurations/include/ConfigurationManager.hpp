/*
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

#ifndef CONFIGURATIONMANAGER_HPP
#define CONFIGURATIONMANAGER_HPP

#include <unordered_map>
#include <variant>
#include <string>
#include <memory>
#include <yaml-cpp/yaml.h>
#include <iostream>
#include <stdexcept>

using ConfigValue = std::variant<std::string, int, bool, std::shared_ptr<std::unordered_map<std::string, ConfigValue>>>;
const std::string CONFIG_FILE_NAME = "worker.yaml";

class ConfigurationManager {
public:
    static ConfigurationManager& getInstance() {
        static ConfigurationManager instance;
        return instance;
    }

    void loadConfigurations(int argc, char* argv[]) {
        loadYaml(CONFIG_FILE_NAME);
        parseCommandLine(argc, argv);
    }

    template <typename T>
    T getValue(const std::string& key, const T& defaultValue) const {
        auto it = configValues.find(key);
        if (it != configValues.end()) {
            if (const T* val = std::get_if<T>(&it->second)) {
                return *val;
            } else {
                throw std::runtime_error("Type mismatch for key: " + key);
            }
        }
        return defaultValue;
    }

private:
    ConfigurationManager() = default;

    void loadYaml(const std::string& filename) {
        try {
            YAML::Node config = YAML::LoadFile(filename);
            for (auto it = config.begin(); it != config.end(); ++it) {
                const std::string& key = it->first.as<std::string>();
                const YAML::Node& value = it->second;

                if (value.IsScalar()) {
                    if (value.Tag() == "!int") {
                        configValues[key] = value.as<int>();
                    } else if (value.Tag() == "!bool") {
                        configValues[key] = value.as<bool>();
                    } else {
                        configValues[key] = value.as<std::string>();
                    }
                } else if (value.IsMap()) {
                    configValues[key] = std::make_shared<std::unordered_map<std::string, ConfigValue>>();
                    auto subMap = std::get<std::shared_ptr<std::unordered_map<std::string, ConfigValue>>>(configValues[key]);
                    for (auto subIt = value.begin(); subIt != value.end(); ++subIt) {
                        (*subMap)[subIt->first.as<std::string>()] = subIt->second.as<std::string>();
                    }
                }
            }
        } catch (const YAML::Exception& e) {
            std::cerr << "Failed to load YAML configuration: " << e.what() << std::endl;
        }
    }

    void parseCommandLine(int argc, char* argv[]) {
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg.rfind("--", 0) == 0) {
                size_t eqPos = arg.find('=');
                if (eqPos != std::string::npos) {
                    std::string key = arg.substr(2, eqPos - 2);
                    std::string value = arg.substr(eqPos + 1);
                    configValues[key] = value;
                }
            }
        }
    }

    std::unordered_map<std::string, ConfigValue> configValues;
};

#endif //NES_CONFIGURATIONMANAGER_H
