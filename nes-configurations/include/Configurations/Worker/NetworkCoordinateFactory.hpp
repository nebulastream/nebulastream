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
#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_NETWORKCOORDINATEFACTORY_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_NETWORKCOORDINATEFACTORY_HPP_

#include <Util/yaml/Yaml.hpp>
#include <map>
#include <memory>
#include <string>

namespace NES::Synthetic::DataTypes::Experimental {
    class NetworkCoordinate;
}// namespace NES::Synthetic::DataTypes::Experimental

namespace NES::Configurations::Synthetic::Index::Experimental {

class NetworkCoordinateFactory {

  public:
    /**
* @brief obtains a network coordinate object by parsing string coordinates
* @param str: Network coordinate string in the format "<x1, x2>"
* @return A network coordinate with the coordinates from the string, or <0, 0> (representing initial Coordinates)
* if the string was empty
*/
    static NES::Synthetic::DataTypes::Experimental::NetworkCoordinate
    createFromString(std::string, std::map<std::string, std::string>& commandLineParams);

    /**
* @brief obtains a network coordinate object from yaml config
* @param yamlConfig: a yaml config obtained from a file containing "fixedLocationCoordinates: <x1, x2>"
* @return A network coordinate with the coordinates from the config entry, or <0, 0> (representing initial coordinates)
* if the field was empty
*/
    static NES::Synthetic::DataTypes::Experimental::NetworkCoordinate createFromYaml(Yaml::Node& yamlConfig);
    //TODO: Currently <0, 0> is assigned for empty input, however a different value must be thought because <0, 0> is not
    // an invalid value in the coordinate system. Finding invalid values are challenging because every double is a valid entry
    // in the coordinate system.
};
}// namespace NES::Configurations::Synthetic::Index::Experimental
#endif // NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_NETWORKCOORDINATEFACTORY_HPP_

