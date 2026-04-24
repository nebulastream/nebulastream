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

#include <cstddef>
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Checksum.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

struct ConfigParametersChecksum
{
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format", "CSV", [](const std::unordered_map<UppercaseString, std::string>&) { return std::optional("CSV"); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<UppercaseString, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    static inline std::unordered_map<UppercaseString, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(FILE_PATH, OUTPUT_FORMAT);
};

}

FMT_OSTREAM(NES::ChecksumSink);
