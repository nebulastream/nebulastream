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

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Field.hpp>

namespace NES
{

Field::Field() = default;

Field::Field(const LogicalOperator& producedBy, Identifier name, DataType dataType)
    : producedBy(std::make_unique<LogicalOperator>(producedBy)), name(std::move(name)), dataType(std::move(dataType))
{
}
Field::Field(const LogicalOperator& producedBy, Identifier name, const DataType::Type dataType) : producedBy(std::make_unique<LogicalOperator>(producedBy)), name(std::move(name)), dataType(dataType)
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
        return *this;
    producedBy = std::make_unique<LogicalOperator>(*other.producedBy);
    name = other.name;
    dataType = other.dataType;
    return *this;
}
Field& Field::operator=(Field&& other) noexcept
{
    if (this == &other)
        return *this;
    producedBy = std::move(other.producedBy);
    name = std::move(other.name);
    dataType = std::move(other.dataType);
    return *this;
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
}