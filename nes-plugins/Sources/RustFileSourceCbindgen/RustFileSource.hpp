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

#include <stop_token>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ostream>
#include "RustFileSourceImpl.hpp"

namespace NES::Sources
{

class RustFileSource : public Source
{
public:
    static constexpr std::string_view NAME = "RustFile";

    explicit RustFileSource(const SourceDescriptor& sourceDescriptor);
    ~RustFileSource() override = default;

    RustFileSource(const RustFileSource&) = delete;
    RustFileSource& operator=(const RustFileSource&) = delete;
    RustFileSource(RustFileSource&&) = delete;
    RustFileSource& operator=(RustFileSource&&) = delete;

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open file socket.
    void open() override;
    /// Close file socket.
    void close() override;

    static NES::Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    rust::RustFileSourceImpl* impl;
};

struct ConfigParametersCSV
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "filePath", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(FILEPATH, config);
        }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(FILEPATH);
};

}
