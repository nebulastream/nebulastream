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

#include <ostream>
#include <Configurations/Descriptor.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{
/// Descriptor that stores the configuration parameters of a specific OutputFormatter instance
/// Currently, there are no parameters that are shared by all types of OutputFormatters.
/// For a specific OutputFormatter, config parameters may be added by creating a ConfigParameters<Type> struct in the respective header.
class OutputFormatterDescriptor final : public Descriptor
{
public:
    ~OutputFormatterDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const OutputFormatterDescriptor& outputFormatterDescriptor);

    /// The float parser option is available for every output formatter type
    static inline const DescriptorConfig::ConfigParameter<std::string> FLOAT_PARSER{
        "float_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLOAT_PARSER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(FLOAT_PARSER);

private:
    /// Add LowerSchemaProvider as friend, so that it can construct the descriptor
    friend class LowerSchemaProvider;
    explicit OutputFormatterDescriptor(DescriptorConfig::Config config);
};
}

FMT_OSTREAM(NES::OutputFormatterDescriptor);
