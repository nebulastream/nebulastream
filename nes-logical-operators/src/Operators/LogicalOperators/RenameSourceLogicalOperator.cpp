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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{

RenameSourceLogicalOperator::RenameSourceLogicalOperator(const std::string& newSourceName, OperatorId id)
    : Operator(id), UnaryLogicalOperator(id), newSourceName(newSourceName)
{
}

bool RenameSourceLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const RenameSourceLogicalOperator*>(&rhs)->getId() == id;
}

bool RenameSourceLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const RenameSourceLogicalOperator*>(&rhs)) {
        return newSourceName == rhsOperator->newSourceName;
    }
    return false;
};

std::string RenameSourceLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "RENAME_STREAM(" << id << ", newSourceName=" << newSourceName << ")";
    return ss.str();
}

bool RenameSourceLogicalOperator::inferSchema()
{
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }
    ///Update output schema by changing the qualifier and corresponding attribute names
    const auto newQualifierName = newSourceName + Schema::ATTRIBUTE_NAME_SEPARATOR;
    for (const auto& field : *outputSchema)
    {
        ///Extract field name without qualifier
        auto fieldName = field->getName();
        ///Add new qualifier name to the field and update the field name
        field->setName(newQualifierName + fieldName);
    }
    return true;
}

std::string RenameSourceLogicalOperator::getNewSourceName() const
{
    return newSourceName;
}

std::shared_ptr<Operator> RenameSourceLogicalOperator::clone() const
{
    auto copy = std::make_shared<RenameSourceLogicalOperator>(newSourceName, id);
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
