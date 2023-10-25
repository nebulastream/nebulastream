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
#include <Configurations/Worker/GeoLocationFactory.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/yaml/Yaml.hpp>
#include <map>

namespace NES::Configurations::Spatial::Index::Experimental {

NES::Spatial::DataTypes::Experimental::GeoLocation
GeoLocationFactory::createFromString(std::string, std::map<std::string, std::string>& commandLineParams) {
    std::string coordStr;
    for (auto it = commandLineParams.begin(); it != commandLineParams.end(); ++it) {
        if (it->first == LOCATION_COORDINATES_CONFIG && !it->second.empty()) {
            coordStr = it->second;
        }
    }
    //if the input string is empty, construct an invalid location
    if (coordStr.empty()) {
        return {200, 200};
    }
    return NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(std::move(coordStr));
}

NES::Spatial::DataTypes::Experimental::GeoLocation GeoLocationFactory::createFromYaml(Yaml::Node& yamlConfig) {
    auto configString = yamlConfig.As<std::string>();
    if (!configString.empty() && configString != "\n") {
        return NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(configString);
    }
    return {};
}
}//namespace NES::Configurations::Spatial::Index::Experimental
