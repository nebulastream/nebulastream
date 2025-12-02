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

#include "VideoSourceValidation.hpp"

#include <array>
#include <fmt/ranges.h>
#include <SourceValidationRegistry.hpp>

namespace NES
{
namespace
{

const std::array requiredFields = std::to_array<std::pair<std::string, DataType::Type>>({
    {"TIMESTAMP", NES::DataType::Type::UINT64},
    {"WIDTH", NES::DataType::Type::UINT64},
    {"HEIGHT", NES::DataType::Type::UINT64},
    {"PIXEL_FORMAT", NES::DataType::Type::UINT64},
    {"IMAGE", NES::DataType::Type::VARSIZED},
});

void validateSchema(const Schema& schema)
{
    if (schema.getNumberOfFields() != requiredFields.size())
    {
        throw CannotOpenSource(
            "Expected schema to have {} fields: {}", requiredFields.size(), fmt::join(requiredFields | std::views::keys, ", "));
    }

    for (const auto& [idx, fieldAndType] : views::enumerate(requiredFields))
    {
        auto actualField = schema.getFieldAt(idx);
        if (!actualField.getUnqualifiedName().ends_with(fieldAndType.first))
        {
            throw CannotOpenSource(
                "{}. schema field `{}` does not match the expected fieldName: `{}`",
                idx,
                actualField.getUnqualifiedName(),
                fieldAndType.first);
        }
        if (actualField.dataType.type != fieldAndType.second)
        {
            throw CannotOpenSource(
                "{}. schema field `{}` type `{}` does not match the expected type: `{}`",
                idx,
                actualField.getUnqualifiedName(),
                actualField.dataType,
                magic_enum::enum_name(fieldAndType.second));
        }
    }
}
}

SourceValidationRegistryReturnType RegisterVideoSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    validateSchema(sourceConfig.schema);
    return DescriptorConfig::validateAndFormat<ConfigParametersVideo>(std::move(sourceConfig.config), "Video");
}

SourceValidationRegistryReturnType RegisterMQTTVideoPlaybackSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    validateSchema(sourceConfig.schema);
    return DescriptorConfig::validateAndFormat<ConfigParametersMQTTVideoPlayback>(std::move(sourceConfig.config), "MQTTVideoPlayback");
}
}
