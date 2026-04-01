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

enum class InputFormatterThreadingMode : uint8_t
{
    SEQUENTIAL,
    PARALLEL
};

/// Descriptor that stores the configuration parameters of a specific InputFormatter instance
/// Currently, there are no parameters that are shared by all types of InputFormatters.
/// For a specific InputFormatter, config parameters may be added by creating a ConfigParameters<Type> struct in the respective header.
class InputFormatterDescriptor final : public Descriptor
{
    static constexpr std::string_view TYPE_STRING{"type"};
public:
    ~InputFormatterDescriptor() = default;

    friend std::ostream& operator<<(std::ostream& out, const InputFormatterDescriptor& inputFormatterDescriptor);

    static std::string getTypeString() { return std::string{TYPE_STRING}; }

    const std::string& getInputFormatterType() const;

    [[nodiscard]] InputFormatterThreadingMode getThreadingMode() const
    {
        return this->getFromConfig(THREADING_MODE);
    }

    static inline const DescriptorConfig::ConfigParameter<std::string> TYPE{
        std::string{TYPE_STRING},
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TYPE, config); }};

    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, InputFormatterThreadingMode> THREADING_MODE{
        "threading_mode",
        EnumWrapper{InputFormatterThreadingMode::PARALLEL},
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(THREADING_MODE, config); }};

    /// A user may overwrite the parsing function for every input type
    static inline const DescriptorConfig::ConfigParameter<std::string> INT8_PARSER{
        "int8_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INT8_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> INT16_PARSER{
        "int16_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INT16_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> INT32_PARSER{
        "int32_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INT32_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> INT64_PARSER{
        "int64_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INT64_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> UINT8_PARSER{
        "uint8_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(UINT8_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> UINT16_PARSER{
        "uint16_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(UINT16_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> UINT32_PARSER{
        "uint32_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(UINT32_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> UINT64_PARSER{
        "uint64_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(UINT64_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> F32_PARSER{
        "f32_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(F32_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> F64_PARSER{
        "f64_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(F64_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> BOOL_PARSER{
        "bool_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BOOL_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> CHAR_PARSER{
        "char_parser",
        "Default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHAR_PARSER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            TYPE,
            THREADING_MODE,
            INT8_PARSER,
            INT16_PARSER,
            INT32_PARSER,
            INT64_PARSER,
            UINT8_PARSER,
            UINT16_PARSER,
            UINT32_PARSER,
            UINT64_PARSER,
            F32_PARSER,
            F64_PARSER,
            BOOL_PARSER,
            CHAR_PARSER);

private:
    friend class SourceCatalog;
    friend struct Unreflector<InputFormatterDescriptor>;
    friend struct Reflector<InputFormatterDescriptor>;

    /// Add LowerSchemaProvider as friend, so that it can construct the descriptor
    std::string inputFormatterType;
    explicit InputFormatterDescriptor(std::string inputFormatterType, DescriptorConfig::Config config);
};

template <>
struct Reflector<InputFormatterDescriptor>
{
    Reflected operator()(const InputFormatterDescriptor& inputFormatterDescriptor) const;
};

template <>
struct Unreflector<InputFormatterDescriptor>
{
    InputFormatterDescriptor operator()(const Reflected& rfl) const;
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
