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
#include <utility>
#include <vector>

#include <DataTypes/LogicalType.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Schema variant whose fields carry a LogicalType instead of a physical DataType.
///
/// Operators upstream of the new logical-type-lowering phase reason in terms of a
/// LogicalSchema; the lowering phase turns it into a primitive Schema by
/// resolving each LogicalType through the LogicalTypeLoweringRegistry and
/// expanding compound types into multiple primitive fields named with the
/// underscore-suffix scheme (`p` of LogicalType `Point` -> `p_x`, `p_y`, `p_z`).
class LogicalSchema
{
public:
    struct Field
    {
        Field() = default;
        Field(std::string name, LogicalType logicalType);

        friend std::ostream& operator<<(std::ostream& os, const Field& field);
        bool operator==(const Field&) const = default;
        [[nodiscard]] std::string getUnqualifiedName() const;

        std::string name;
        LogicalType logicalType{"UNDEFINED", {}, NES::DataType::NULLABLE::IS_NULLABLE};
    };

    constexpr static auto ATTRIBUTE_NAME_SEPARATOR = Schema::ATTRIBUTE_NAME_SEPARATOR;

    explicit LogicalSchema() = default;
    ~LogicalSchema() = default;

    [[nodiscard]] bool operator==(const LogicalSchema& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const LogicalSchema& schema);

    LogicalSchema addField(std::string name, LogicalType logicalType);

    /// Replaces the logical type of an existing field. Returns false if no such field.
    [[nodiscard]] bool replaceTypeOfField(const std::string& name, LogicalType logicalType);

    /// Lift an existing primitive Schema into a LogicalSchema by mapping each
    /// DataType to its same-named primitive LogicalType. Used at the legacy
    /// boundary when an operator still produces a Schema but the surrounding
    /// pipeline expects LogicalSchema.
    [[nodiscard]] static LogicalSchema fromSchema(const Schema& schema);

    /// Lower this LogicalSchema to a primitive Schema. Only valid when every
    /// field's LogicalType is a primitive that round-trips through
    /// LogicalType::toPhysical (i.e., its name matches a DataType::Type enum).
    /// Compound types must be expanded via the LogicalTypeLoweringRegistry at
    /// the dedicated lowering phase before calling this.
    [[nodiscard]] Schema toPrimitiveSchema() const;

    [[nodiscard]] std::optional<Field> getFieldByName(const std::string& fieldName) const;
    [[nodiscard]] Field getFieldAt(size_t index) const;
    [[nodiscard]] bool contains(const std::string& qualifiedFieldName) const;

    [[nodiscard]] std::optional<std::string> getSourceNameQualifier() const;
    [[nodiscard]] std::string getQualifierNameForSystemGeneratedFieldsWithSeparator() const;

    [[nodiscard]] bool hasFields() const;
    [[nodiscard]] size_t getNumberOfFields() const;
    [[nodiscard]] std::vector<std::string> getFieldNames() const;
    [[nodiscard]] const std::vector<Field>& getFields() const;
    void appendFieldsFromOtherSchema(const LogicalSchema& otherSchema);
    [[nodiscard]] bool renameField(const std::string& oldFieldName, std::string_view newFieldName);

    [[nodiscard]] auto begin() const -> decltype(std::declval<std::vector<Field>>().cbegin());
    [[nodiscard]] auto end() const -> decltype(std::declval<std::vector<Field>>().cend());

private:
    std::vector<Field> fields;
    std::unordered_map<std::string, size_t> nameToField;
};

/// Returns a copy of the input logical schema without any source qualifier on
/// the schema fields.
LogicalSchema withoutSourceQualifier(const LogicalSchema& input);

namespace detail
{
struct ReflectedLogicalField
{
    std::string name;
    LogicalType logicalType;
};

struct ReflectedLogicalSchema
{
    std::vector<LogicalSchema::Field> fields;
};
}

template <>
struct Reflector<LogicalSchema::Field>
{
    Reflected operator()(const LogicalSchema::Field& field) const;
};

template <>
struct Unreflector<LogicalSchema::Field>
{
    LogicalSchema::Field operator()(const Reflected& rfl) const;
};

template <>
struct Reflector<LogicalSchema>
{
    Reflected operator()(const LogicalSchema& schema) const;
};

template <>
struct Unreflector<LogicalSchema>
{
    LogicalSchema operator()(const Reflected& rfl) const;
};

}

FMT_OSTREAM(NES::LogicalSchema);
FMT_OSTREAM(NES::LogicalSchema::Field);
