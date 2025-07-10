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
#include "cxx.hpp"
#include "lib.rs.hpp"

namespace NES::Sources
{

class RustFileSourceCXX : public Source
{
public:
    static constexpr std::string_view NAME = "RustFileCXX";

    explicit RustFileSourceCXX(const SourceDescriptor& sourceDescriptor);
    ~RustFileSourceCXX() override = default;

    RustFileSourceCXX(const RustFileSourceCXX&) = delete;
    RustFileSourceCXX& operator=(const RustFileSourceCXX&) = delete;
    RustFileSourceCXX(RustFileSourceCXX&&) = delete;
    RustFileSourceCXX& operator=(RustFileSourceCXX&&) = delete;

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open file socket.
    void open() override;
    /// Close file socket.
    void close() override;

    static NES::Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    rust::Box<Rust::RustFileSourceImpl> impl;
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
