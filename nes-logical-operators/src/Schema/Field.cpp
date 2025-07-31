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

#include <Schema/Field.hpp>

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <folly/hash/Hash.h>

namespace NES
{

Field::Field(const LogicalOperator& producedBy, Identifier name, DataType dataType)
    : producedBy(std::make_unique<LogicalOperator>(producedBy)), name(std::move(name)), dataType(std::move(dataType))
{
}

Field::Field(const LogicalOperator& producedBy, Identifier name, const DataType::Type dataType)
    : producedBy(std::make_unique<LogicalOperator>(producedBy)), name(std::move(name)), dataType(dataType)
{
}

Field::Field(const Field& other)
    : producedBy(std::make_unique<LogicalOperator>(*other.producedBy)), name(other.name), dataType(other.dataType)
{
}

Field::Field(Field&& other) noexcept
    : producedBy(std::move(other.producedBy)), name(std::move(other.name)), dataType(std::move(other.dataType))
{
}

Field& Field::operator=(const Field& other)
{
    if (this == &other)
    {
        return *this;
    }
    producedBy = std::make_unique<LogicalOperator>(*other.producedBy);
    name = other.name;
    dataType = other.dataType;
    return *this;
}

Field& Field::operator=(Field&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    producedBy = std::move(other.producedBy);
    name = std::move(other.name);
    dataType = std::move(other.dataType);
    return *this;
}

IdentifierListBase<1> Field::getFullyQualifiedName() const
{
    return name;
}

Field::~Field() = default;

const LogicalOperator& Field::getProducedBy() const
{
    return *producedBy;
}

std::ostream& operator<<(std::ostream& os, const Field& field)
{
    return os << fmt::format("Field(name: {}, DataType: {})", field.name, field.dataType);
}

bool Field::operator==(const Field& other) const
{
    return other.name == name && other.dataType == dataType && *other.producedBy == *producedBy;
}

WeakField::~WeakField() = default;

WeakField::WeakField(const WeakLogicalOperator& producedBy, Identifier name, DataType dataType)
    : producedBy(std::make_unique<WeakLogicalOperator>(producedBy)), name(std::move(name)), dataType(std::move(dataType))
{
}

WeakField::WeakField(const WeakLogicalOperator& producedBy, Identifier name, DataType::Type dataType)
    : producedBy(std::make_unique<WeakLogicalOperator>(producedBy)), name(std::move(name)), dataType(dataType)
{
}

WeakField::WeakField(const WeakField& other)
    : producedBy(std::make_unique<WeakLogicalOperator>(*other.producedBy)), name(other.name), dataType(other.dataType)
{
}

WeakField::WeakField(WeakField&& other) noexcept = default;

WeakField& WeakField::operator=(const WeakField& other)
{
    if (this == &other)
    {
        return *this;
    }
    producedBy = std::make_unique<WeakLogicalOperator>(*other.producedBy);
    name = other.name;
    dataType = other.dataType;
    return *this;
}

WeakField& WeakField::operator=(WeakField&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    producedBy = std::move(other.producedBy);
    name = std::move(other.name);
    dataType = std::move(other.dataType);
    return *this;
}

WeakField::WeakField(const Field& other)
    : producedBy(std::make_unique<WeakLogicalOperator>(*other.producedBy)), name(other.name), dataType(other.dataType)
{
}

WeakField::WeakField(Field&& other)
    : producedBy(std::make_unique<WeakLogicalOperator>(*other.producedBy)), name(std::move(other.name)), dataType(std::move(other.dataType))
{
}

WeakField& WeakField::operator=(const Field& other)
{
    producedBy = std::make_unique<WeakLogicalOperator>(*other.producedBy);
    name = other.name;
    dataType = other.dataType;
    return *this;
}

WeakField& WeakField::operator=(Field&& other)
{
    producedBy = std::make_unique<WeakLogicalOperator>(*other.producedBy);
    name = std::move(other.name);
    dataType = std::move(other.dataType);
    return *this;
}

std::ostream& operator<<(std::ostream& os, const WeakField& field)
{
    return os << fmt::format("WeakField(name: {}, DataType: {})", field.name, field.dataType);
}

std::optional<Field> WeakField::tryLock() const
{
    return producedBy->tryLock().transform([&](auto&& logicalOperator) { return Field{std::move(logicalOperator), name, dataType}; });
}

Field WeakField::lock() const
{
    return Field{producedBy->lock(), name, dataType};
}

const WeakLogicalOperator& WeakField::getProducedBy() const
{
    return *producedBy;
}

UnboundFieldBase<1> Field::unbound() const
{
    return UnboundFieldBase<1>{name, dataType};
}

std::function<UnboundFieldBase<1>(Field)> Field::unbinder()
{
    return [](const Field& field) { return UnboundFieldBase<1>{field.name, field.dataType}; };
}

std::function<Field(UnboundFieldBase<1>)> Field::binder(LogicalOperator logicalOperator)
{
    return [logicalOperator](const UnboundFieldBase<1>& unboundField)
    { return Field{logicalOperator, unboundField.getFullyQualifiedName(), unboundField.getDataType()}; };
}
}

std::size_t std::hash<NES::Field>::operator()(const NES::Field& field) const noexcept
{
    return folly::hash::hash_combine(field.getLastName(), field.getProducedBy());
}

std::size_t std::hash<NES::WeakField>::operator()(const NES::WeakField& field) const noexcept
{
    return std::hash<NES::Identifier>{}(field.getLastName());
}
