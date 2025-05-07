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

class CSVInputFormatter final : public InputFormatter<FieldOffsets, /* IsNativeFormat */ false>
{
public:
    static constexpr std::string_view NAME = "CSV";

    explicit CSVInputFormatter(const InputFormatterDescriptor& descriptor, size_t numberOfFieldsInSchema);
    ~CSVInputFormatter() override = default;

    CSVInputFormatter(const CSVInputFormatter&) = delete;
    CSVInputFormatter& operator=(const CSVInputFormatter&) = delete;
    CSVInputFormatter(CSVInputFormatter&&) = delete;
    CSVInputFormatter& operator=(CSVInputFormatter&&) = delete;

    void
    setupFieldAccessFunctionForBuffer(FieldOffsets& fieldOffsets, const RawTupleBuffer& rawBuffer, const TupleMetaData&) const override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;
    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

private:
    std::string tupleDelimiter;
    std::string fieldDelimiter;
    size_t numberOfFieldsInSchema;
};

struct ConfigParametersCSV
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> TUPLE_DELIMITER{
        "tupleDelimiter",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            if (auto tupleDelimiter = Configurations::DescriptorConfig::tryGet(TUPLE_DELIMITER, config))
            {
                if (tupleDelimiter.value().size() == 1)
                {
                    return {tupleDelimiter};
                }
                NES_ERROR("The CSVInputFormatter expects that the size of the tuple delimiter is exactly one byte.");
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
                NES_ERROR("The CSVInputFormatter expects that the size of the field delimiter is exactly one byte.");
                return std::nullopt;
            }
            return std::nullopt;
        }};
    static inline const std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(TUPLE_DELIMITER, FIELD_DELIMITER);
};
}
