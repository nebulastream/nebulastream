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
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources
{

class MemorySource final : public Source
{
public:
    static constexpr std::string_view NAME = "Memory";

    explicit MemorySource(const SourceDescriptor& sourceDescriptor);
    ~MemorySource() override = default;

    MemorySource(const MemorySource&) = delete;
    MemorySource& operator=(const MemorySource&) = delete;
    MemorySource(MemorySource&&) = delete;
    MemorySource& operator=(MemorySource&&) = delete;

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Read the full file into memory
    void open() override;
    void close() override;

    /// validates and formats a string to string configuration
    static NES::Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;


private:
    std::string filePath;
    std::vector<uint8_t> _buffer;
    std::atomic<size_t> totalNumBytesRead;
    std::atomic<size_t> fileSize;
};

struct ConfigParametersMemory
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "filePath",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(FILEPATH);
};


}
