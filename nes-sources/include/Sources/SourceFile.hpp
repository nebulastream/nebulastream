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

#include <fstream>
#include <optional>
#include <string>
#include <unordered_map>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::Sources
{

class SourceFile final : public Source
{
public:
    static inline const std::string NAME = "CSV";

    explicit SourceFile(const SourceDescriptor& sourceDescriptor);
    ~SourceFile() override = default;

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer) override;

    /// Open file socket.
    void open() override;
    /// Close file socket.
    void close() override;

    /// validates and formats a string to string configuration
    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string>&& config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::ifstream inputFile;
    std::string filePath;
};

struct ConfigParametersCSV
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "filePath", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(FILEPATH, config);
        }};
    static inline const Configurations::DescriptorConfig::ConfigParameter<bool> SKIP_HEADER{
        "skipHeader", false, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(SKIP_HEADER, config);
        }};
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> DELIMITER{
        "delimiter", ",", [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(DELIMITER, config);
        }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(FILEPATH, SKIP_HEADER, DELIMITER);
};

}
