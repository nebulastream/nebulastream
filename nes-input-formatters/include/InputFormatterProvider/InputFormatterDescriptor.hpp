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

enum class SequenceShredderMode : uint8_t
{
    LOCK_FREE,
    LOCKING
};

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

    [[nodiscard]] InputFormatterThreadingMode getThreadingMode() const
    {
        if (const auto threadingMode = this->tryGetFromConfig<EnumWrapper>(THREADING_MODE))
        {
            return threadingMode.value().asEnum<InputFormatterThreadingMode>().value();
        }
        return InputFormatterThreadingMode::PARALLEL;
    }

    [[nodiscard]] SequenceShredderMode getSequenceShredderMode() const { return this->getFromConfig(SEQUENCE_SHREDDER_MODE); }

    static inline const DescriptorConfig::ConfigParameter<std::string> TYPE{
        std::string{TYPE_STRING},
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TYPE, config); }};

    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, InputFormatterThreadingMode> THREADING_MODE{
        "threading_mode",
        EnumWrapper{InputFormatterThreadingMode::PARALLEL},
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(THREADING_MODE, config); }};

    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, SequenceShredderMode> SEQUENCE_SHREDDER_MODE{
        "sequence_shredder_mode",
        EnumWrapper{SequenceShredderMode::LOCK_FREE},
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(SEQUENCE_SHREDDER_MODE, config); }};

    /// A user may overwrite the parsing function for every input type
    static inline const DescriptorConfig::ConfigParameter<std::string> INT8_PARSER{
        "int8_parser",
        "DefaultINT8",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INT8_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> INT16_PARSER{
        "int16_parser",
        "DefaultINT16",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INT16_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> INT32_PARSER{
        "int32_parser",
        "DefaultINT32",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INT32_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> INT64_PARSER{
        "int64_parser",
        "DefaultINT64",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INT64_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> UINT8_PARSER{
        "uint8_parser",
        "DefaultUINT8",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(UINT8_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> UINT16_PARSER{
        "uint16_parser",
        "DefaultUINT16",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(UINT16_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> UINT32_PARSER{
        "uint32_parser",
        "DefaultUINT32",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(UINT32_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> UINT64_PARSER{
        "uint64_parser",
        "DefaultUINT64",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(UINT64_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> F32_PARSER{
        "f32_parser",
        "DefaultF32",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(F32_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> F64_PARSER{
        "f64_parser",
        "DefaultF64",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(F64_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> BOOL_PARSER{
        "bool_parser",
        "DefaultBOOL",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(BOOL_PARSER, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> CHAR_PARSER{
        "char_parser",
        "DefaultCHAR",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CHAR_PARSER, config); }};

    static inline const DescriptorConfig::ConfigParameter<bool> LAZY_INT_OVERLOADS{
        "lazy_int_overloads",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(LAZY_INT_OVERLOADS, config); }};

    static inline const DescriptorConfig::ConfigParameter<bool> LAZY_UINT_OVERLOADS{
        "lazy_uint_overloads",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(LAZY_UINT_OVERLOADS, config); }};

    static inline const DescriptorConfig::ConfigParameter<bool> LAZY_FLOAT_OVERLOADS{
        "lazy_float_overloads",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(LAZY_FLOAT_OVERLOADS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            TYPE,
            THREADING_MODE,
            SEQUENCE_SHREDDER_MODE,
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
            CHAR_PARSER,
            LAZY_INT_OVERLOADS,
            LAZY_UINT_OVERLOADS,
            LAZY_FLOAT_OVERLOADS);

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
