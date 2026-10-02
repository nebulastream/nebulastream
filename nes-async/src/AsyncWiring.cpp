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

#include <Async/AsyncWiring.hpp>

#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <magic_enum/magic_enum.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// Ordered so the encoding is stable, which keeps generated plans comparable in tests.
struct EncodedField
{
    std::string name;
    std::string type;
};

struct EncodedSchema
{
    std::vector<EncodedField> fields;
};

}

std::string encodeConfig(const std::unordered_map<std::string, std::string>& config)
{
    /// std::map rather than the unordered one, so the result does not depend on hash order.
    const std::map<std::string, std::string> ordered{config.begin(), config.end()};
    return rfl::json::write(ordered);
}

std::unordered_map<std::string, std::string> decodeConfig(const std::string_view encoded)
{
    if (encoded.empty())
    {
        return {};
    }

    const auto parsed = rfl::json::read<std::map<std::string, std::string>>(std::string{encoded});
    if (!parsed)
    {
        throw CannotDeserialize("Cannot read the executor configuration: {}", parsed.error().what());
    }
    return {parsed.value().begin(), parsed.value().end()};
}

std::string encodeSchema(const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    EncodedSchema encoded;
    encoded.fields.reserve(schema.size());
    for (const auto& field : schema)
    {
        encoded.fields.emplace_back(
            EncodedField{
                .name = static_cast<const Identifier&>(field.getFullyQualifiedName()).asCanonicalString(),
                .type = std::string{magic_enum::enum_name(field.getDataType().type)}});
    }
    return rfl::json::write(encoded);
}

Schema<UnqualifiedUnboundField, Ordered> decodeSchema(const std::string_view encoded)
{
    const auto parsed = rfl::json::read<EncodedSchema>(std::string{encoded});
    if (!parsed)
    {
        throw CannotDeserialize("Cannot read the handoff schema: {}", parsed.error().what());
    }

    std::vector<UnqualifiedUnboundField> fields;
    fields.reserve(parsed.value().fields.size());
    for (const auto& [name, type] : parsed.value().fields)
    {
        const auto dataType = magic_enum::enum_cast<DataType::Type>(type);
        if (!dataType.has_value())
        {
            throw CannotDeserialize("Handoff schema names an unknown data type '{}'", type);
        }
        fields.emplace_back(Identifier::parse(name), DataTypeProvider::provideDataType(dataType.value(), DataType::NULLABLE::NOT_NULLABLE));
    }

    auto schema = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(fields));
    if (!schema.has_value())
    {
        throw CannotDeserialize(
            "Handoff schema has colliding field names: {}", Schema<UnqualifiedUnboundField, Ordered>::createCollisionString(schema.error()));
    }
    return std::move(schema).value();
}

}
