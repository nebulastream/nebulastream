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

#include <array>
#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <DataTypes/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <Hl7SimdKernel.hpp>
#include <UncompiledHl7FIF.hpp>
#include <UncompiledInputFormatIndexer.hpp>

namespace NES
{

/// Mirrors the compiled ConfigParametersHL7InputFormatIndexer (see HL7InputFormatIndexer.hpp for the format
/// model), minus the compiled-path-only assume_clean_strings (a lazy/varsized passthrough characteristic the
/// interpreted parse loop never consults). Escape sequences (\r, \xNN) are unescaped at validation.
struct ConfigParametersUncompiledHL7
{
    static inline const DescriptorConfig::ConfigParameter<std::string> MESSAGE_DELIMITER{
        "message_delimiter",
        "\x1C"
        "\r",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(MESSAGE_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> SEGMENT_DELIMITER{
        "segment_delimiter",
        "\r",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SEGMENT_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> FIELD_DELIMITER{
        "field_delimiter",
        "|",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FIELD_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> COMPONENT_DELIMITER{
        "component_delimiter",
        "^",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(COMPONENT_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> SUBCOMPONENT_DELIMITER{
        "subcomponent_delimiter",
        "&",
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(SUBCOMPONENT_DELIMITER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            InputFormatterDescriptor::parameterMap,
            MESSAGE_DELIMITER,
            SEGMENT_DELIMITER,
            FIELD_DELIMITER,
            COMPONENT_DELIMITER,
            SUBCOMPONENT_DELIMITER);
};

/// The delimiter model of the compiled HL7MetaData, reduced to what the uncompiled task consumes.
struct UncompiledHL7MetaData
{
    explicit UncompiledHL7MetaData(const InputFormatterDescriptor& config, const Schema&)
        : messageDelimiter(config.getFromConfig(ConfigParametersUncompiledHL7::MESSAGE_DELIMITER))
    {
        const auto segment = config.getFromConfig(ConfigParametersUncompiledHL7::SEGMENT_DELIMITER);
        const auto field = config.getFromConfig(ConfigParametersUncompiledHL7::FIELD_DELIMITER);
        const auto component = config.getFromConfig(ConfigParametersUncompiledHL7::COMPONENT_DELIMITER);
        const auto subcomponent = config.getFromConfig(ConfigParametersUncompiledHL7::SUBCOMPONENT_DELIMITER);
        PRECONDITION(
            segment.size() == 1 && field.size() == 1,
            "The HL7 segment and field delimiters must be exactly 1 byte (got segment '{}', field '{}')",
            segment,
            field);
        PRECONDITION(
            component.size() <= 1 && subcomponent.size() <= 1,
            "The HL7 component/subcomponent delimiters must be 1 byte, or empty to disable splitting");
        PRECONDITION(
            not messageDelimiter.empty() && messageDelimiter.size() <= 2,
            "SequentialUncompiledHL7 requires a 1- or 2-byte message delimiter (got size {})",
            messageDelimiter.size());
        structuralBytes = segment + field + component + subcomponent;
        for (size_t i = 0; i < structuralBytes.size(); ++i)
        {
            PRECONDITION(
                structuralBytes.find(structuralBytes[i], i + 1) == std::string::npos,
                "The HL7 structural delimiters (segment/field/component/subcomponent) must be pairwise distinct");
        }
        PRECONDITION(
            structuralBytes.find(messageDelimiter[0]) == std::string::npos,
            "SequentialUncompiledHL7 requires the message delimiter's first byte to not be a structural delimiter");
        /// Fixed-arity view of the structural class for the SIMD kernel's four broadcast compares; disabled
        /// component/subcomponent slots duplicate the segment delimiter (a duplicate compare ORs to the same mask).
        structuralChars
            = {segment.front(),
               field.front(),
               component.empty() ? segment.front() : component.front(),
               subcomponent.empty() ? segment.front() : subcomponent.front()};
    }

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return messageDelimiter; }

    [[nodiscard]] const std::array<char, 4>& getStructuralChars() const { return structuralChars; }

private:
    std::string messageDelimiter;
    std::string structuralBytes;
    std::array<char, 4> structuralChars{};
};

/// Uncompiled (interpreted) twin of the SequentialHL7/SIMDHL7 formatter -- the ablation baseline rung for HL7
/// input, and via the packed-XML configuration (1-byte '\n' delimiter, structural class {<,>}) for XML input.
/// The INDEXING phase is identical to the compiled indexer (the SIMD flatten into the in-place event band,
/// arity validation); only the parse phase differs: the generic interpreted per-field loop
/// (processUncompiledTuple) instead of the compiled strided read.
class SequentialUncompiledHL7InputFormatIndexer : public UncompiledInputFormatIndexer<SequentialUncompiledHL7InputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialUncompiledHL7";
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;
    static constexpr bool IsSequential = true;

    using UncompiledIndexerMetaData = UncompiledHL7MetaData;
    using UncompiledFieldIndexFunctionType = UncompiledHl7FIF;

    explicit SequentialUncompiledHL7InputFormatIndexer(size_t numberOfFieldsInSchema);
    ~SequentialUncompiledHL7InputFormatIndexer() = default;

    void indexRawBuffer(
        UncompiledHl7FIF& fieldIndexFunction, const UncompiledRawTupleBuffer& rawBuffer, const UncompiledHL7MetaData& metaData) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialUncompiledHL7InputFormatIndexer& obj);

private:
    size_t numberOfFieldsInSchema;
    /// Best block kernel for this CPU, resolved once at construction (runtime dispatch); do not call per buffer.
    SimdHl7::ComputeBlocksFn computeBlocks;
};

}
