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
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/PhysicalLayout.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

class Schema;

/// Runtime/physical schema produced by lowering a logical `Schema`.
///
/// Each field carries a `PhysicalType` (one or more `(suffix, DataType::Type)`
/// components plus field-level nullability). Compound logical types like
/// `Point` keep their grouping at this level — a Point field has three
/// components — so consumers that care about the original field grouping
/// can still see it. Runtime sites that only see flat record cells walk
/// `field.physicalType.components` and synthesize the per-cell record-field
/// name `field.name + component.suffix`.
class PhysicalSchema final
{
public:
    struct Field
    {
        std::string name;
        PhysicalType physicalType;

        bool operator==(const Field&) const = default;
        [[nodiscard]] std::string getUnqualifiedName() const;

        friend std::ostream& operator<<(std::ostream& os, const Field& field);
    };

    /// Field-name qualifier separator (matches logical `Schema`).
    constexpr static auto ATTRIBUTE_NAME_SEPARATOR = "$";

    PhysicalSchema() = default;

    /// Build a `PhysicalSchema` field-by-field. Used by test fixtures and by
    /// sites that already know their physical layout (e.g. the result-checker
    /// parsing a CSV header). Production lowering goes through
    /// `lower(const Schema&)`.
    PhysicalSchema& addField(std::string name, PhysicalType physicalType);
    /// Convenience overload for the common scalar case.
    PhysicalSchema& addField(std::string name, DataType::Type type, bool nullable = false);

    [[nodiscard]] bool operator==(const PhysicalSchema& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const PhysicalSchema& schema);

    [[nodiscard]] bool hasFields() const;
    /// Number of physical cells across all fields (a `Point` field counts as
    /// 3). Use `getNumberOfLogicalFields()` for the field-level count.
    [[nodiscard]] std::size_t getNumberOfFields() const;
    [[nodiscard]] std::size_t getNumberOfLogicalFields() const;
    [[nodiscard]] const std::vector<Field>& getFields() const;
    /// Spread record cell names: every component contributes
    /// `field.name + component.suffix`. Used by physical operators that
    /// project / scan record cells.
    [[nodiscard]] std::vector<std::string> getFieldNames() const;
    [[nodiscard]] std::optional<Field> getFieldByName(const std::string& fieldName) const;
    [[nodiscard]] Field getFieldAt(std::size_t index) const;
    /// Match against field names AND record cell names (`field.name + suffix`).
    [[nodiscard]] bool contains(const std::string& qualifiedFieldName) const;

    [[nodiscard]] std::optional<std::string> getSourceNameQualifier() const;
    [[nodiscard]] std::string getQualifierNameForSystemGeneratedFieldsWithSeparator() const;

    [[nodiscard]] auto begin() const { return fields.cbegin(); }
    [[nodiscard]] auto end() const { return fields.cend(); }

private:
    std::vector<Field> fields;
};

/// Lower a logical `Schema` into a runtime `PhysicalSchema`. Each logical
/// field's `LogicalType` is looked up in `LogicalTypeLoweringRegistry` to
/// obtain its `PhysicalType` (component layout + nullability). Idempotent
/// for already-flat schemas.
[[nodiscard]] PhysicalSchema lower(const Schema& logical);

/// Byte size of a single tuple of the given physical schema. Sums the
/// physical-cell sizes (`getSizeInBytesWithNull`) for every component of
/// every field.
[[nodiscard]] std::size_t physicalTupleByteSize(const PhysicalSchema& schema);

/// Map a primitive physical `DataType::Type` to the prototype LogicalType
/// name (`INTEGER` / `FLOAT` / `BOOL` / `TEXT`). Used by the sink
/// `SchemaFormatter` and the result-checker so the CSV header round-trips
/// through `LogicalTypeRegistry`.
[[nodiscard]] std::string primitiveLogicalTypeName(DataType::Type type);

}

FMT_OSTREAM(NES::PhysicalSchema);
FMT_OSTREAM(NES::PhysicalSchema::Field);
