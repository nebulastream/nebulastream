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

#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Util/Logger.hpp>
#include <Util/yaml/Yaml.hpp>
#include <string>
#include <sys/stat.h>

namespace NES {
SourceConfig::SourceConfig() { NES_INFO("NesSourceConfig: Init source config object."); }

void SourceConfig::overwriteConfigWithYAMLFileInput(string filePath) {

    struct stat buffer {};
    if (!filePath.empty() && !(stat(filePath.c_str(), &buffer) == -1)) {

        NES_INFO("NesSourceConfig: Using config file with path: " << filePath << " .");

        Yaml::Node config;
        Yaml::Parse(config, filePath.c_str());
        try {
            setSourceConfig(config["sourceConfig"].As<string>());
            setSourceType(config["sourceType"].As<string>());
            setSourceFrequency(config["sourceFrequency"].As<uint16_t>());
            setNumberOfBuffersToProduce(config["numberOfBuffersToProduce"].As<uint64_t>());
            setNumberOfTuplesToProducePerBuffer(config["numberOfTuplesToProducePerBuffer"].As<uint16_t>());
            setPhysicalStreamName(config["physicalStreamName"].As<string>());
            setLogicalStreamName(config["logicalStreamName"].As<string>());
            setSkipHeader(config["skipHeader"].As<bool>());
            setLogLevel(config["logLevel"].As<string>());
        } catch (exception& e) {
            NES_ERROR("NesSourceConfig: Error while initializing configuration parameters from YAML file. Keeping default "
                      "values.");
            resetSourceOptions();
        }
    } else {
        NES_ERROR("NesWorkerConfig: No file path was provided or file could not be found at " << filePath << ".");
        NES_INFO("Keeping default values for Coordinator Config.");
    }
}

void SourceConfig::overwriteConfigWithCommandLineInput(map<string, string> inputParams) {
    try {
        for (auto it = inputParams.begin(); it != inputParams.end(); ++it) {
            NES_INFO("NesWorkerConfig: Using command line input parameter " << it->first << " with value " << it->second);
            if (it->first == "--sourceType") {
                setSourceType(it->second);
            } else if (it->first == "--sourceConfig") {
                setSourceConfig(it->second);
            } else if (it->first == "--sourceFrequency") {
                setSourceFrequency(stoi(it->second));
            } else if (it->first == "--numberOfBuffersToProduce") {
                setNumberOfBuffersToProduce(stoi(it->second));
            } else if (it->first == "--numberOfTuplesToProducePerBuffer") {
                setNumberOfTuplesToProducePerBuffer(stoi(it->second));
            } else if (it->first == "--physicalStreamName") {
                setPhysicalStreamName(it->second);
            } else if (it->first == "--logicalStreamName") {
                setLogicalStreamName(it->second);
            } else if (it->first == "--skipHeader") {
                setSkipHeader((it->second == "true"));
            } else if (it->first == "--logLevel") {
                setLogLevel(it->second);
            }
        }
    } catch (exception e) {
        NES_ERROR("NesWorkerConfig: Error while initializing configuration parameters from command line. Keeping default "
                  "values.");
        resetSourceOptions();
    }
}
void SourceConfig::resetSourceOptions() {
    setSourceConfig(sourceConfig.getDefaultValue());
    setSourceType(sourceType.getDefaultValue());
    setSourceFrequency(sourceFrequency.getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce.getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer.getDefaultValue());
    setPhysicalStreamName(physicalStreamName.getDefaultValue());
    setLogicalStreamName(logicalStreamName.getDefaultValue());
    setSkipHeader(skipHeader.getDefaultValue());
    setLogLevel(logLevel.getDefaultValue());
}

const ConfigOption<std::string>& NES::SourceConfig::getSourceType() const { return sourceType; }
const ConfigOption<std::string>& NES::SourceConfig::getSourceConfig() const { return sourceConfig; }
const ConfigOption<uint32_t>& NES::SourceConfig::getSourceFrequency() const { return sourceFrequency; }
const ConfigOption<uint32_t>& NES::SourceConfig::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }
const ConfigOption<uint32_t>& NES::SourceConfig::getNumberOfTuplesToProducePerBuffer() const {
    return numberOfTuplesToProducePerBuffer;
}
const ConfigOption<std::string>& NES::SourceConfig::getPhysicalStreamName() const { return physicalStreamName; }
const ConfigOption<std::string>& NES::SourceConfig::getLogicalStreamName() const { return logicalStreamName; }
const ConfigOption<bool>& NES::SourceConfig::getSkipHeader() const { return skipHeader; }
const ConfigOption<std::string>& NES::SourceConfig::getLogLevel() const { return logLevel; }

void SourceConfig::setSourceType(const string& sourceType) { SourceConfig::sourceType.setValue(sourceType); }
void SourceConfig::setSourceConfig(const string& sourceConfig) { SourceConfig::sourceConfig.setValue(sourceConfig); }
void SourceConfig::setSourceFrequency(const uint32_t& sourceFrequency) {
    SourceConfig::sourceFrequency.setValue(sourceFrequency);
}
void SourceConfig::setNumberOfBuffersToProduce(const uint32_t& numberOfBuffersToProduce) {
    SourceConfig::numberOfBuffersToProduce.setValue(numberOfBuffersToProduce);
}
void SourceConfig::setNumberOfTuplesToProducePerBuffer(const uint32_t& numberOfTuplesToProducePerBuffer) {
    SourceConfig::numberOfTuplesToProducePerBuffer.setValue(numberOfTuplesToProducePerBuffer);
}
void SourceConfig::setPhysicalStreamName(const string& physicalStreamName) {
    SourceConfig::physicalStreamName.setValue(physicalStreamName);
}
void SourceConfig::setLogicalStreamName(const string& logicalStreamName) {
    SourceConfig::logicalStreamName.setValue(logicalStreamName);
}
void SourceConfig::setSkipHeader(const bool& skipHeader) { SourceConfig::skipHeader.setValue(skipHeader); }
void SourceConfig::setLogLevel(const string& logLevel) { SourceConfig::logLevel.setValue(logLevel); }

}// namespace NES