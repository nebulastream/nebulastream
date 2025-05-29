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
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterDescriptor.hpp>

#include <FieldOffsets.hpp>

namespace NES::InputFormatters
{
/// If we consider the MSH header of the message to be at offset = 0 (e.g. MSH|^~\&|1)
static constexpr size_t HL7_INDEX_OF_FIRST_NON_DELIMITER_VALUE_IN_MESSAGE = 9;

constexpr auto HL7_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::TWO;

class HL7InputFormatter final : public InputFormatter<FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>, /* IsNativeFormat */ false>
{
public:
    static constexpr std::string_view NAME = "HL7";

    explicit HL7InputFormatter(const InputFormatterDescriptor& descriptor, size_t numberOfFieldsInSchema);
    ~HL7InputFormatter() override = default;

    HL7InputFormatter(const HL7InputFormatter&) = delete;
    HL7InputFormatter& operator=(const HL7InputFormatter&) = delete;
    HL7InputFormatter(HL7InputFormatter&&) = delete;
    HL7InputFormatter& operator=(HL7InputFormatter&&) = delete;

    void setupFieldAccessFunctionForBuffer(
        FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const TupleMetaData&) const override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;
    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);



private:
    /// Any segment starts with this
    std::string segmentDelimiter;
    /// segmentDelimiter + MSH
    std::string messageDelimiter;
    size_t numberOfFieldsInSchema;
};

struct ConfigParametersHL7
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> SEGMENT_DELIMITER{
        "segmentDelimiter",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            if (auto segmentDelimiter = Configurations::DescriptorConfig::tryGet(SEGMENT_DELIMITER, config))
            {
                if (segmentDelimiter.value().size() == 1)
                {
                    return {segmentDelimiter};
                }
                NES_ERROR("Currently, the HL7InputFormatter expects 1 byte segment delimiters.");
                return std::nullopt;
            }
            return std::nullopt;
        }};
    static inline const std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(SEGMENT_DELIMITER);
};
}
