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

#include <memory>
#include <string>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAssignmentBinaryLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/MapLogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

MapLogicalOperator::MapLogicalOperator(const std::shared_ptr<FieldAssignmentBinaryLogicalFunction>& mapFunction, OperatorId id)
    : Operator(id), UnaryLogicalOperator(id), mapFunction(mapFunction)
{
}

std::shared_ptr<FieldAssignmentBinaryLogicalFunction> MapLogicalOperator::getMapFunction() const
{
    return mapFunction;
}

bool MapLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const MapLogicalOperator*>(&rhs)->getId() == id;
}

bool MapLogicalOperator::operator==(Operator const& rhs) const
{
    if (auto rhsOperator = dynamic_cast<const MapLogicalOperator*>(&rhs))
    {
        return this->mapFunction == rhsOperator->mapFunction;
    }
    return false;
};

bool MapLogicalOperator::inferSchema()
{
    /// infer the default input and output schema
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }

    /// use the default input schema to calculate the out schema of this operator.
    mapFunction->inferStamp(*getInputSchema());

    const auto assignedField = mapFunction->getField();
    if (std::string fieldName = assignedField->getFieldName(); outputSchema->getFieldByName(fieldName))
    {
        /// The assigned field is part of the current schema.
        /// Thus we check if it has the correct type.
        NES_TRACE("MAP Logical Operator: the field {} is already in the schema, so we updated its type.", fieldName);
        outputSchema->replaceField(fieldName, assignedField->getStamp());
    }
    else
    {
        /// The assigned field is not part of the current schema.
        /// Thus we extend the schema by the new attribute.
        NES_TRACE("MAP Logical Operator: the field {} is not part of the schema, so we added it.", fieldName);
        outputSchema->addField(fieldName, assignedField->getStamp());
    }
    return true;
}

std::string MapLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "MAP(opId: " << id << ": predicate: " << *mapFunction << ")";
    return ss.str();
}

std::shared_ptr<Operator> MapLogicalOperator::clone() const
{
    auto copy = std::make_shared<MapLogicalOperator>(Util::as<FieldAssignmentBinaryLogicalFunction>(mapFunction->clone()), id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

}
