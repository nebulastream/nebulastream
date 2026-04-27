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

// Lightweight config surface for the File source plugin. Includes only
// descriptor types — no runtime headers — so FileSourceValidation.cpp can
// compile into nes-sources-validation without dragging in nes-memory / nes-runtime.
// FileSource.hpp re-exports this for the runtime side.

#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

static constexpr std::string_view SYSTEST_FILE_PATH_PARAMETER = "FILE_PATH";

struct FileSourceName
{
    static constexpr std::string_view NAME = "File";
};

struct ConfigParametersCSV
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string(SYSTEST_FILE_PATH_PARAMETER),
        std::nullopt,
        [](const std::unordered_map<UppercaseString, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<UppercaseString, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
