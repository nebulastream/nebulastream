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

#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Reflection.hpp>

namespace NES
{
/// Descriptor that stores the configuration parameters of a specific InputFormatter instance
/// For a specific InputFormatter, config parameters may be added by creating a ConfigParameters<Type> struct in the respective header.
class InputFormatterDescriptor final : public Descriptor
{
    static constexpr std::string_view TYPE_STRING{"type"};

public:
    explicit InputFormatterDescriptor(std::string inputFormatterType, DescriptorConfig::Config config);
    ~InputFormatterDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const InputFormatterDescriptor& inputFormatterDescriptor);

    static std::string getTypeString() { return std::string{TYPE_STRING}; }

    const std::string& getInputFormatterType() const;

    static inline const DescriptorConfig::ConfigParameter<std::string> TYPE{
        std::string{TYPE_STRING},
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TYPE, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(TYPE);

private:
    friend class SourceCatalog;
    friend struct Unreflector<InputFormatterDescriptor>;
    friend struct Reflector<InputFormatterDescriptor>;

    /// Add LowerSchemaProvider as friend, so that it can construct the descriptor
    std::string inputFormatterType;
};

template <>
struct Reflector<InputFormatterDescriptor>
{
    Reflected operator()(const InputFormatterDescriptor& inputFormatterDescriptor, const ReflectionContext& context) const;
};

template <>
struct Unreflector<InputFormatterDescriptor>
{
    InputFormatterDescriptor operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

}

namespace NES::detail
{
struct ReflectedInputFormatterDescriptor
{
    std::string inputFormatterType;
    Reflected config;
};
}

FMT_OSTREAM(NES::InputFormatterDescriptor);
