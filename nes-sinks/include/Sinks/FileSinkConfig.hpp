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

// Validation-only surface for the File sink plugin. Keeps the config parameters
// and the sink name constant away from the runtime header (which pulls
// PipelineExecutionContext, TupleBuffer, etc.) so FileSinkValidation.cpp can
// compile into nes-sinks-validation without dragging the runtime in.

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES
{

struct FileSinkName
{
    static constexpr std::string_view NAME = "File";
};

struct ConfigParametersFile
{
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<UppercaseString, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    static inline const DescriptorConfig::ConfigParameter<bool> APPEND{
        "append",
        false,
        [](const std::unordered_map<UppercaseString, std::string>& config) { return DescriptorConfig::tryGet(APPEND, config); }};

    static inline std::unordered_map<UppercaseString, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, FILE_PATH, APPEND);
};

}
