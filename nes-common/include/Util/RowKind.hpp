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

#include <cstdint>
#include <optional>
#include <string_view>
#include <Identifiers/Identifier.hpp>

namespace NES
{

/// The kind of a row in an updating (changelog) stream. The values are those of Flink's RowKind.
/// An updating stream is a retract stream: UPDATE_BEFORE carries the full row that an earlier INSERT or UPDATE_AFTER emitted,
/// so a consumer can withdraw it without knowing a key. The final table of a stream is the signed multiset of its rows.
enum class RowKind : uint8_t
{
    INSERT = 0,
    UPDATE_BEFORE = 1,
    UPDATE_AFTER = 2,
    DELETE = 3,
};

/// Name of the field that carries the RowKind as a UINT8. Only updating streams have it, so a schema without it is append-only.
/// Unquoted SQL identifiers cannot contain '$', which keeps the field out of the way of user queries.
inline constexpr std::string_view ROW_KIND_FIELD_NAME = "$ROWKIND";

inline Identifier rowKindIdentifier()
{
    return Identifier::parse(std::string{ROW_KIND_FIELD_NAME});
}

/// +1 for rows that add to the final table (INSERT, UPDATE_AFTER), -1 for rows that withdraw from it (UPDATE_BEFORE, DELETE).
constexpr int64_t rowKindSign(const uint8_t kind)
{
    return 1 - (2 * static_cast<int64_t>(kind & 1U));
}

/// The row kind as Flink prints it: +I, -U, +U or -D.
constexpr std::string_view rowKindSymbol(const uint8_t kind)
{
    switch (kind)
    {
        case static_cast<uint8_t>(RowKind::INSERT):
            return "+I";
        case static_cast<uint8_t>(RowKind::UPDATE_BEFORE):
            return "-U";
        case static_cast<uint8_t>(RowKind::UPDATE_AFTER):
            return "+U";
        case static_cast<uint8_t>(RowKind::DELETE):
            return "-D";
        default:
            return "?";
    }
}

/// Inverse of rowKindSymbol.
constexpr std::optional<uint8_t> parseRowKindSymbol(const std::string_view symbol)
{
    for (uint8_t kind = 0; kind <= static_cast<uint8_t>(RowKind::DELETE); ++kind)
    {
        if (rowKindSymbol(kind) == symbol)
        {
            return kind;
        }
    }
    return std::nullopt;
}

/// True if the schema belongs to an updating stream, i.e. it carries the row kind field.
template <typename SchemaType>
bool isUpdatingStream(const SchemaType& schema)
{
    return schema.contains(rowKindIdentifier());
}

}
