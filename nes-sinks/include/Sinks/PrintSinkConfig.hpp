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

// Validation-only surface for the Print sink plugin. See FileSinkConfig.hpp for
// the rationale — parameters / sink name kept outside the runtime header so the
// validation TU doesn't pull PipelineExecutionContext, BackpressureChannel etc.

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES
{

struct PrintSinkName
{
    static constexpr std::string_view NAME = "Print";
};

struct ConfigParametersPrint
{
    static inline const DescriptorConfig::ConfigParameter<uint32_t> INGESTION{
        "ingestion",
        0,
        [](const std::unordered_map<UppercaseString, std::string>& config) { return DescriptorConfig::tryGet(INGESTION, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format",
        std::nullopt,
        [](const std::unordered_map<UppercaseString, std::string>& config) { return DescriptorConfig::tryGet(OUTPUT_FORMAT, config); }};

    static inline std::unordered_map<UppercaseString, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(INGESTION, OUTPUT_FORMAT);
};

}
