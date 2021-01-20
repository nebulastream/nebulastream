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

#ifndef NES_SOURCECONFIG_HPP
#define NES_SOURCECONFIG_HPP

#include <Configurations/ConfigOption.hpp>
#include <map>
#include <string>

namespace NES {

class SourceConfig {

  public:
    /**
    * @brief constructor to create a new source option object initialized with default values as set below
    */
    SourceConfig();

    /**
    * @brief overwrite the default configurations with those loaded from a yaml file
    * @param filePath file path to the yaml file
    */
    void overwriteConfigWithYAMLFileInput(string filePath);
    /**
    * @brief overwrite the default and the yaml file configurations with command line input
    * @param inputParams map with key=command line parameter and value = value
    */
    void overwriteConfigWithCommandLineInput(map<string, string> inputParams);
    /**
    * @brief resets all options to default values
    */
    void resetSourceOptions();

    /**
    * @brief gets a ConfigOption object with sourceType
    */
    const ConfigOption<std::string>& getSourceType() const;
    /**
    * @brief set the value for sourceType with the appropriate data format
    */
    void setSourceType(const string& sourceType);
    /**
    * @brief gets a ConfigOption object with sourceConfig
    */
    const ConfigOption<std::string>& getSourceConfig() const;
    /**
    * @brief set the value for sourceConfig with the appropriate data format
    */
    void setSourceConfig(const std::string& sourceConfig);
    /**
* @brief gets a ConfigOption object with sourceFrequency
*/
    const ConfigOption<uint32_t>& getSourceFrequency() const;
    /**
 * @brief set the value for sourceFrequency with the appropriate data format
 */
    void setSourceFrequency(const uint32_t& sourceFrequency);
    /**
* @brief gets a ConfigOption object with numberOfBuffersToProduce
*/
    const ConfigOption<uint32_t>& getNumberOfBuffersToProduce() const;
    /**
 * @brief set the value for numberOfBuffersToProduce with the appropriate data format
 */
    void setNumberOfBuffersToProduce(const uint32_t& numberOfBuffersToProduce);
    /**
* @brief gets a ConfigOption object with numberOfTuplesToProducePerBuffer
*/
    const ConfigOption<uint32_t>& getNumberOfTuplesToProducePerBuffer() const;
    /**
 * @brief set the value for numberOfTuplesToProducePerBuffer with the appropriate data format
 */
    void setNumberOfTuplesToProducePerBuffer(const uint32_t& numberOfTuplesToProducePerBuffer);
    /**
* @brief gets a ConfigOption object with physicalStreamName
*/
    const ConfigOption<std::string>& getPhysicalStreamName() const;
    /**
 * @brief set the value for physicalStreamName with the appropriate data format
 */
    void setPhysicalStreamName(const string& physicalStreamName);
    /**
    * @brief gets a ConfigOption object with logicalStreamName
    */
    const ConfigOption<std::string>& getLogicalStreamName() const;
    /**
    * @brief set the value for logicalStreamName with the appropriate data format
    */
    void setLogicalStreamName(const string& logicalStreamName);
    /**
    * @brief gets a ConfigOption object with skipHeader
    */
    const ConfigOption<bool>& getSkipHeader() const;
    /**
    * @brief set the value for skipHeader with the appropriate data format
    */
    void setSkipHeader(const bool& skipHeader);
    /**
    * @brief gets a ConfigOption object with logLevel
    */
    const ConfigOption<std::string>& getLogLevel() const;
    /**
    * @brief set the value for logLevel with the appropriate data format
    */
    void setLogLevel(const std::string& logLevel);

    /**
 * @brief definition of options and information for coordinator config options
 */
  private:
    ConfigOption<std::string> sourceType = ConfigOption(
        "sourceType", std::string("DefaultSource"),
        "Type of the Source (available options: DefaultSource, CSVSource, BinarySource, YSBSource).", "string", false);

    ConfigOption<std::string> sourceConfig = ConfigOption(
        "sourceConfig", std::string("1"),
        "Source configuration. Options depend on source type. See Source Configurations on our wiki page for further details.",
        "string", false);

    ConfigOption<uint32_t> sourceFrequency =
        ConfigOption("sourceFrequency", uint32_t(1), "Sampling frequency of the source.", "uint32_t", false);

    ConfigOption<uint32_t> numberOfBuffersToProduce =
        ConfigOption("numberOfBuffersToProduce", uint32_t(1), "Number of buffers to produce.", "uint32_t", false);

    ConfigOption<uint32_t> numberOfTuplesToProducePerBuffer = ConfigOption(
        "numberOfTuplesToProducePerBuffer", uint32_t(1), "Number of tuples to produce per buffer.", "uint32_t", false);

    ConfigOption<std::string> physicalStreamName =
        ConfigOption("physicalStreamName", std::string("default_physical"), "Physical name of the stream.", "string", false);

    ConfigOption<std::string> logicalStreamName =
        ConfigOption("logicalStreamName", std::string("default_logical"), "Logical name of the stream.", "string", false);

    ConfigOption<bool> skipHeader = ConfigOption("skipHeader", false, "Skip first line of the file.", "bool", false);

    ConfigOption<std::string> logLevel =
        ConfigOption("logLevel", std::string("LOG_DEBUG"), "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) ",
                     "string", false);
};

}// namespace NES

#endif//NES_SOURCECONFIG_HPP
