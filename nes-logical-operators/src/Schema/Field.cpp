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
#include "Serialization/OperatorMapping.hpp"

namespace NES
{

Field::Field(const LogicalOperator& producedBy, Identifier name, DataType dataType)
    : producedBy(producedBy->shared_from_this()), name(std::move(name)), dataType(std::move(dataType))
{
}

Field::Field(const LogicalOperator& producedBy, Identifier name, const DataType::Type dataType)
    : producedBy(producedBy->shared_from_this()), name(std::move(name)), dataType(dataType)
{
}

IdentifierListBase<1> Field::getFullyQualifiedName() const
{
    return name;
}

LogicalOperator Field::getProducedBy() const
{
    return LogicalOperator{producedBy};
}

Reflected Reflector<Field>::operator()(const Field& field) const
{
    return reflect(
        detail::ReflectedField{.producedBy = field.getProducedBy().getId(), .name = field.getLastName(), .dataType = field.getDataType()});
}

Unreflector<Field>::Unreflector(ContextType operatorMapping) : operatorMapping(std::move(operatorMapping))
{
}

Field Unreflector<Field>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, name, type] = context.unreflect<detail::ReflectedField>(reflected);
    auto fromOperatorOpt = operatorMapping->getOperator(id, context);
    if (!fromOperatorOpt)
    {
        throw CannotDeserialize("Could not deserialize Field because operator id {} was not present in context", id);
    }
    return Field{*fromOperatorOpt, name, type};
}

std::ostream& operator<<(std::ostream& os, const Field& field)
{
    return os << fmt::format("Field(name: {}, DataType: {})", field.name, field.dataType);
}

bool Field::operator==(const Field& other) const
{
    bool nameEq = other.name == name;
    bool dataTypeEq = other.dataType == dataType;
    bool producedByEq = other.producedBy == producedBy;
    return nameEq && dataTypeEq && producedByEq;
}

UnqualifiedUnboundField Field::unbound() const
{
    return UnqualifiedUnboundField{name, dataType};
}

}

std::size_t std::hash<NES::Field>::operator()(const NES::Field& field) const noexcept
{
    return std::hash<NES::Identifier>{}(field.getLastName());
}