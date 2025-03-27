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
#include <DataTypes/Schema.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{

RenameSourceOperator::RenameSourceOperator(const IdentifierList& newSourceName, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), newSourceName(newSourceName)
{
}

bool RenameSourceOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<RenameSourceOperator>(rhs)->getId() == id;
}

bool RenameSourceOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<RenameSourceOperator>(rhs))
    {
        const auto otherRename = NES::Util::as<RenameSourceOperator>(rhs);
        return newSourceName == otherRename->newSourceName;
    }
    return false;
};

std::string RenameSourceOperator::toString() const
{
    std::stringstream ss;
    ss << "RENAME_STREAM(" << id << ", newSourceName=" << newSourceName.toString() << ")";
    return ss.str();
}

bool RenameSourceOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    ///Update output schema by changing the qualifier and corresponding attribute names
    for (auto& field : outputSchema.getFields())
    {
        ///Add new qualifier name to the field and update the field name
        outputSchema.renameField(field.name, newSourceName + field.name);
    }
    return true;
}

IdentifierList RenameSourceOperator::getNewSourceName() const
{
    return newSourceName;
}

std::shared_ptr<Operator> RenameSourceOperator::copy()
{
    auto copy = std::make_shared<RenameSourceOperator>(newSourceName, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

}
