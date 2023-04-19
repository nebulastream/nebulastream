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

#include <Experimental/Benchmarking/Parsing/SynopsisConfiguration.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES::ASP {

SynopsisConfigurationPtr SynopsisConfiguration::create(SYNOPSIS_TYPE type, size_t width,
                                                                     size_t height, size_t windowSize) {
    auto configurationOptions = std::make_shared<SynopsisConfiguration>();
    configurationOptions->type = type;
    configurationOptions->width = width;
    configurationOptions->height = height;
    configurationOptions->windowSize = windowSize;
    return configurationOptions;
}


SynopsisConfigurationPtr SynopsisConfiguration::createArgumentsFromYamlNode(Yaml::Node& synopsisArgumentsNode) {

    auto type = magic_enum::enum_cast<SYNOPSIS_TYPE>(synopsisArgumentsNode["type"].As<std::string>()).value();
    auto width = synopsisArgumentsNode["width"].IsNone() ? 1 : synopsisArgumentsNode["width"].As<size_t>();
    auto height = synopsisArgumentsNode["height"].IsNone() ? 1 : synopsisArgumentsNode["height"].As<size_t>();
    auto windowSize = synopsisArgumentsNode["windowSize"].IsNone() ? 1 : synopsisArgumentsNode["windowSize"].As<size_t>();

    return SynopsisConfiguration::create(type, width, height, windowSize);
}

std::string SynopsisConfiguration::getHeaderAsCsv() {
    return "synopsis_type,synopsis_width,synopsis_height,synopsis_windowSize";
}

std::string SynopsisConfiguration::getValuesAsCsv() {
    std::stringstream stringStream;
    stringStream << magic_enum::enum_name(type.getValue())
                 << "," << width
                 << "," << height
                 << "," << windowSize;

    return stringStream.str();
}

std::string SynopsisConfiguration::toString() {
    std::stringstream stringStream;
    stringStream << "type (" << magic_enum::enum_name(type.getValue()) << ") "
                 << "width (" << width << ") "
                 << "height (" << height << ") "
                 << "windowSize (" << windowSize << ")";

    return stringStream.str();
}

} // namespace NES::ASP