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
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/NetworkCoordinateFactory.hpp>
#include <Util/Latency/NetworkCoordinate.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>

namespace NES::Configurations::Synthetic::Index::Experimental {

        NES::Synthetic::DataTypes::Experimental::NetworkCoordinate
        NetworkCoordinateFactory::createFromString(std::string, std::map<std::string, std::string>& commandLineParams) {
            std::string coordStr;
            for (auto it = commandLineParams.begin(); it != commandLineParams.end(); ++it) {
                if (it->first == NETWORK_COORDINATES_CONFIG && !it->second.empty()) {
                    coordStr = it->second;
                }
            }
            //if the input string is empty, construct an invalid location
            if (coordStr.empty()) {
                return {0, 0};
            }
            return NES::Synthetic::DataTypes::Experimental::NetworkCoordinate::fromString(std::move(coordStr));
        }

        NES::Synthetic::DataTypes::Experimental::NetworkCoordinate NetworkCoordinateFactory::createFromYaml(Yaml::Node& yamlConfig) {
            auto configString = yamlConfig.As<std::string>();
            if (!configString.empty() && configString != "\n") {
                return NES::Synthetic::DataTypes::Experimental::NetworkCoordinate::fromString(configString);
            }
            return {};
        }
    }//namespace NES::Configurations::Synthetic::Index::Experimental
