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

#include <memory>
#include <ostream>
#include <span>

#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include "DataTypes/UnboundField.hpp"

namespace NES
{
struct Field
{
    ~Field();
    Field(const LogicalOperator& producedBy, Identifier name, DataType dataType);
    Field(const LogicalOperator& producedBy, Identifier name, DataType::Type dataType);
    Field(const Field& other);
    Field(Field&& other) noexcept;
    Field& operator=(const Field& other);
    Field& operator=(Field&& other) noexcept;

    friend std::ostream& operator<<(std::ostream& os, const Field& field);
    bool operator==(const Field& other) const;

    [[nodiscard]] const LogicalOperator& getProducedBy() const;

    [[nodiscard]] Identifier getLastName() const { return name; }

    /// Placeholder function that also correctly includes prefixes from the relation it stems from
    [[nodiscard]] IdentifierListBase<1> getFullyQualifiedName() const;

    [[nodiscard]] DataType getDataType() const { return dataType; }

    /// TODO template this method over IdListExtent when adding either named relations or compound types
    [[nodiscard]] UnboundFieldBase<1> unbound() const;
    static std::function<UnboundFieldBase<1>(Field)> unbinder();
    static std::function<Field(UnboundFieldBase<1>)> binder(LogicalOperator logicalOperator);

private:
    /// I believe we currently need this pointer, even though the LogicalOperator already contains a pointer for type erasure, due to how the types reference each other
    /// But, I think we can get rid of this unnecessary level of indirection when we replace the inheritance of Operators with Concepts
    std::unique_ptr<LogicalOperator> producedBy;
    Identifier name;
    DataType dataType{};
    friend struct WeakField;
};
}

template <>
struct std::hash<NES::Field>
{
    std::size_t operator()(const NES::Field& field) const noexcept;
};
FMT_OSTREAM(NES::Field);

namespace NES
{
static_assert(FieldConcept<Field, 1>);

struct WeakField
{
    ~WeakField();
    WeakField(const WeakLogicalOperator& producedBy, Identifier name, DataType dataType);
    WeakField(const WeakLogicalOperator& producedBy, Identifier name, DataType::Type dataType);
    WeakField(const WeakField& other);
    WeakField(WeakField&& other) noexcept;
    WeakField& operator=(const WeakField& other);
    WeakField& operator=(WeakField&& other) noexcept;

    WeakField(const Field& other);
    WeakField(Field&& other);
    WeakField& operator=(const Field& other);
    WeakField& operator=(Field&& other);

    friend std::ostream& operator<<(std::ostream& os, const WeakField& field);

    [[nodiscard]] std::optional<Field> tryLock() const;
    [[nodiscard]] Field lock() const;

    [[nodiscard]] const WeakLogicalOperator& getProducedBy() const;

    [[nodiscard]] Identifier getLastName() const { return name; }

    [[nodiscard]] DataType getDataType() const { return dataType; }

private:
    std::unique_ptr<WeakLogicalOperator> producedBy;
    Identifier name;
    DataType dataType{};
};
}

template <>
struct std::hash<NES::WeakField>
{
    std::size_t operator()(const NES::WeakField& field) const noexcept;
};

FMT_OSTREAM(NES::WeakField);
