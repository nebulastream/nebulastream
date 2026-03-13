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
#include "Identifiers/Identifiers.hpp"

namespace NES
{
class OperatorMapping;

struct Field
{
    Field(const LogicalOperator& producedBy, Identifier name, DataType dataType);

    friend std::ostream& operator<<(std::ostream& os, const Field& field);
    bool operator==(const Field& other) const;

    [[nodiscard]] LogicalOperator getProducedBy() const;

    [[nodiscard]] Identifier getLastName() const { return name; }

    [[nodiscard]] IdentifierListBase<1> getFullyQualifiedName() const;

    [[nodiscard]] DataType getDataType() const { return dataType; }

    [[nodiscard]] UnqualifiedUnboundField unbound() const;

private:
    std::shared_ptr<const detail::ErasedLogicalOperator> producedBy;
    Identifier name;
    DataType dataType{};
    friend struct WeakField;
};

namespace detail
{
struct ReflectedField
{
    OperatorId producedBy;
    Identifier name;
    DataType dataType;
};
}

template <>
struct Reflector<Field>
{
    Reflected operator()(const Field& field) const;
};

template <>
struct Unreflector<Field>
{
    using ContextType = std::shared_ptr<OperatorMapping>;
    std::shared_ptr<OperatorMapping> operatorMapping;
    explicit Unreflector(ContextType operatorMapping);
    Field operator()(const Reflected& reflected, const ReflectionContext& context) const;
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
}
