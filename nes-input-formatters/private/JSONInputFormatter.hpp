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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterDescriptor.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Util/Logger/Logger.hpp>
#include <FieldOffsets.hpp>

namespace NES::InputFormatters
{
constexpr auto JSON_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::TWO;

class JSONInputFormatter final : public InputFormatter<FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>, /* IsNativeFormat */ false>
{
public:
    static constexpr std::string_view NAME = "JSON";

    explicit JSONInputFormatter(const InputFormatterDescriptor& descriptor, size_t numberOfFieldsInSchema);
    ~JSONInputFormatter() override = default;

    JSONInputFormatter(const JSONInputFormatter&) = delete;
    JSONInputFormatter& operator=(const JSONInputFormatter&) = delete;
    JSONInputFormatter(JSONInputFormatter&&) = delete;
    JSONInputFormatter& operator=(JSONInputFormatter&&) = delete;

    void
    setupFieldAccessFunctionForBuffer(FieldOffsets<JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const TupleMetaData&) const override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;
    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

private:
    std::string tupleDelimiter;
    std::string fieldDelimiter;
    size_t numberOfFieldsInSchema;
};

struct ConfigParametersJSON
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> TUPLE_DELIMITER{
        "tupleDelimiter",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            if (auto tupleDelimiter = Configurations::DescriptorConfig::tryGet(TUPLE_DELIMITER, config))
            {
                if (tupleDelimiter.value().size() == 1 and tupleDelimiter.value() == "\n")
                {
                    return {tupleDelimiter};
                }
                NES_ERROR("The JSONInputFormatter expects that the tuple delimiter is a newline '\\n'.");
                return std::nullopt;
            }
            return std::nullopt;
        }};
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> FIELD_DELIMITER{
        "fieldDelimiter",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            if (auto fieldDelimiter = Configurations::DescriptorConfig::tryGet(FIELD_DELIMITER, config))
            {
                if (fieldDelimiter.value().size() == 1)
                {
                    return {fieldDelimiter};
                }
                NES_ERROR("The JSONInputFormatter expects that the size of the field delimiter is exactly one byte.");
                return std::nullopt;
            }
            return std::nullopt;
        }};
    static inline const std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(TUPLE_DELIMITER, FIELD_DELIMITER);
};
}
