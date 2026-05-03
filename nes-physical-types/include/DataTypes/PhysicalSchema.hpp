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
#include <vector>
#include <DataTypes/LogicalType.hpp>
#include <DataTypes/Nullable.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

class Schema;

/// Runtime/physical schema produced by lowering a logical `Schema`.
///
/// All fields are flat primitives — compound logical types (e.g. `Point`)
/// have already been expanded into their primitive components — and each
/// field's `LogicalType` is restricted to the prototype primitive vocabulary
/// (`INTEGER` / `FLOAT` / `BOOL` / `TEXT`).
///
/// Constructed only through `lower(const Schema&)`. The runtime/IO layer
/// (physical operators, nautilus, sinks, output formatters, input formatters)
/// works exclusively with this type so that no API silently relies on a
/// "schema that happens to already be lowered".
///
/// Initial implementation: thin wrapper over the existing flat `Schema` —
/// every method proxies to it. Once consumers are fully migrated, the inner
/// `Schema` can be replaced with a richer representation (offsets, layout
/// info, ...) without touching call sites.
class PhysicalSchema final
{
public:
    struct Field
    {
        std::string name;
        LogicalType logicalType{"UNDEFINED", {}, Nullable::IS_NULLABLE};

        bool operator==(const Field&) const = default;
        [[nodiscard]] std::string getUnqualifiedName() const;

        friend std::ostream& operator<<(std::ostream& os, const Field& field);
    };

    /// Field-name qualifier separator (matches logical `Schema`).
    constexpr static auto ATTRIBUTE_NAME_SEPARATOR = "$";

    PhysicalSchema() = default;

    [[nodiscard]] bool operator==(const PhysicalSchema& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const PhysicalSchema& schema);

    [[nodiscard]] bool hasFields() const;
    [[nodiscard]] std::size_t getNumberOfFields() const;
    [[nodiscard]] const std::vector<Field>& getFields() const;
    [[nodiscard]] std::vector<std::string> getFieldNames() const;
    [[nodiscard]] std::optional<Field> getFieldByName(const std::string& fieldName) const;
    [[nodiscard]] Field getFieldAt(std::size_t index) const;
    [[nodiscard]] bool contains(const std::string& qualifiedFieldName) const;

    [[nodiscard]] std::optional<std::string> getSourceNameQualifier() const;
    [[nodiscard]] std::string getQualifierNameForSystemGeneratedFieldsWithSeparator() const;

    [[nodiscard]] auto begin() const { return fields.cbegin(); }
    [[nodiscard]] auto end() const { return fields.cend(); }

private:
    friend PhysicalSchema lower(const Schema& logical);
    explicit PhysicalSchema(std::vector<Field> fields);

    std::vector<Field> fields;
};

/// Lower a logical `Schema` into a runtime `PhysicalSchema`.
///
/// Each field is looked up in `LogicalTypeLoweringRegistry`. Compound logical
/// types (e.g. `Point`) yield N primitive fields whose names are
/// `<original-name><component-suffix>` (so a `Point` field `p` becomes `p_X`,
/// `p_Y`, `p_Z`); scalar logical types yield a single field with the same
/// name (the registered scalar layout uses an empty suffix).
///
/// Idempotent for already-flat schemas: scalar types lower to themselves.
[[nodiscard]] PhysicalSchema lower(const Schema& logical);

/// Byte size of a single tuple of the given physical schema. Sums
/// `getSizeInBytesWithNull()` of every primitive component (so a 3-component
/// `Point` contributes 24 bytes).
[[nodiscard]] std::size_t physicalTupleByteSize(const PhysicalSchema& schema);

}

FMT_OSTREAM(NES::PhysicalSchema);
FMT_OSTREAM(NES::PhysicalSchema::Field);
