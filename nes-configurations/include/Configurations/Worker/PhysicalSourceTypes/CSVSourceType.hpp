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

#pragma once

#include <map>
#include <string>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES
{

class CSVSourceType;
using CSVSourceTypePtr = std::shared_ptr<CSVSourceType>;

/// Configuration object for csv source config
/// define configurations for a csv source, i.e. this source reads from data from a csv file
class CSVSourceType : public PhysicalSourceType
{
public:
    ~CSVSourceType() noexcept override = default;

    static CSVSourceTypePtr create(const std::string& logicalSourceName, std::map<std::string, std::string> sourceConfigMap);

    static CSVSourceTypePtr create(const std::string& logicalSourceName, Yaml::Node yamlConfig);

    static CSVSourceTypePtr create(const std::string& logicalSourceName);

    std::string toString() override;

    bool equal(PhysicalSourceTypePtr const& other) override;

    void reset() override;

    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getFilePath() const;

    void setFilePath(const std::string& filePath);

    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<bool>> getSkipHeader() const;

    void setSkipHeader(bool skipHeader);

    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<std::string>> getDelimiter() const;

    void setDelimiter(const std::string& delimiter);

    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getNumberOfBuffersToProduce() const;

    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    [[nodiscard]] std::shared_ptr<Configurations::ConfigurationOption<uint32_t>> getNumberOfTuplesToProducePerBuffer() const;

    void setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer);

private:
    explicit CSVSourceType(const std::string& logicalSourceName, std::map<std::string, std::string> sourceConfigMap);

    explicit CSVSourceType(const std::string& logicalSourceName, Yaml::Node yamlConfig);

    CSVSourceType(const std::string& logicalSourceName);

    Configurations::StringConfigOption filePath;
    Configurations::BoolConfigOption skipHeader;
    Configurations::StringConfigOption delimiter;
    Configurations::IntConfigOption numberOfBuffersToProduce;
    Configurations::IntConfigOption numberOfTuplesToProducePerBuffer;
};

} /// namespace NES
