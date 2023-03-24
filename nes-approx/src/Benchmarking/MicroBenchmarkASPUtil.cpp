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
#include <Benchmarking/Util.hpp>
#include <Util/yaml/Yaml.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::ASP::Util {
std::string parseCsvFileFromYaml(const std::string& yamlFileName) {
    Yaml::Node configFile;
    Yaml::Parse(configFile, yamlFileName.c_str());
    if (configFile.IsNone() || configFile["csvFile"].IsNone()) {
        NES_THROW_RUNTIME_ERROR("Could not get csv file from yaml file!");
    }

    return configFile["csvFile"].As<std::string>();
}

}