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

#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Util/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

DefaultSourceTypePtr DefaultSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<DefaultSourceType>(DefaultSourceType(std::move(sourceConfigMap)));
}

DefaultSourceTypePtr DefaultSourceType::create() { return std::make_shared<DefaultSourceType>(DefaultSourceType()); }

DefaultSourceTypePtr DefaultSourceType::create(ryml::NodeRef yamlConfig) {
    return std::make_shared<DefaultSourceType>(DefaultSourceType(std::move(yamlConfig)));
}

DefaultSourceType::DefaultSourceType()
    : PhysicalSourceType(DEFAULT_SOURCE), numberOfBuffersToProduce(Configurations::ConfigurationOption<uint32_t>::create(
                                              Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                              1,
                                              "Number of buffers to produce.")),
      sourceFrequency(Configurations::ConfigurationOption<uint32_t>::create(Configurations::SOURCE_FREQUENCY_CONFIG,
                                                                            1,
                                                                            "Sampling frequency of the source.")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

DefaultSourceType::DefaultSourceType(std::map<std::string, std::string> sourceConfigMap) : DefaultSourceType() {}

DefaultSourceType::DefaultSourceType(ryml::NodeRef sourceTypeConfig) : DefaultSourceType() {}

std::string DefaultSourceType::toString() {
    std::stringstream ss;
    ss << "Default Source Type =>{\n";
    ss << Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG << ":" << numberOfBuffersToProduce;
    ss << Configurations::SOURCE_FREQUENCY_CONFIG << ":" << sourceFrequency;
    ss << "\n}";
    return ss.str();
}

bool DefaultSourceType::equal(const PhysicalSourceTypePtr& other) { return !other->instanceOf<DefaultSourceType>(); }

const Configurations::IntConfigOption& DefaultSourceType::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

const Configurations::IntConfigOption& DefaultSourceType::getSourceFrequency() const { return sourceFrequency; }

}// namespace NES