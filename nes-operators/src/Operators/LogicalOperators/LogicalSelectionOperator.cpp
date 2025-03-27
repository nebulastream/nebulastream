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
#include <utility>
#include <vector>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

LogicalSelectionOperator::LogicalSelectionOperator(std::shared_ptr<NodeFunction> predicate, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), predicate(std::move(predicate))
{
}

std::shared_ptr<NodeFunction> LogicalSelectionOperator::getPredicate() const
{
    return predicate;
}

void LogicalSelectionOperator::setPredicate(std::shared_ptr<NodeFunction> newPredicate)
{
    predicate = std::move(newPredicate);
}

bool LogicalSelectionOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalSelectionOperator>(rhs)->getId() == id;
}

bool LogicalSelectionOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<LogicalSelectionOperator>(rhs))
    {
        const auto filterOperator = NES::Util::as<LogicalSelectionOperator>(rhs);
        return predicate->equal(filterOperator->predicate);
    }
    return false;
};

std::string LogicalSelectionOperator::toString() const
{
    std::stringstream ss;
    ss << "FILTER(opId: " << id << ": predicate: " << *predicate << ")";
    return ss.str();
}

bool LogicalSelectionOperator::inferSchema()
{
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    predicate->inferStamp(inputSchema);
    if (!predicate->isPredicate())
    {
        throw CannotInferSchema("FilterLogicalOperator: the filter expression is not a valid predicate");
    }
    return true;
}

std::shared_ptr<Operator> LogicalSelectionOperator::copy()
{
    auto copy = std::make_shared<LogicalSelectionOperator>(predicate->deepCopy(), id);
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

float LogicalSelectionOperator::getSelectivity() const
{
    return selectivity;
}
void LogicalSelectionOperator::setSelectivity(float newSelectivity)
{
    selectivity = newSelectivity;
}

std::vector<IdentifierList> LogicalSelectionOperator::getFieldNamesUsedByFilterPredicate() const
{
    NES_TRACE("LogicalSelectionOperator: Find all field names used in filter operator");

    ///vector to save the names of all the fields that are used in this predicate
    std::vector<IdentifierList> fieldsInPredicate;

    ///iterator to go over all the fields of the predicate
    const DepthFirstNodeIterator depthFirstNodeIterator(predicate);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr)
    {
        ///NodeFunctionif it finds a fieldAccess this means that the predicate uses this specific field that comes from any source
        if (NES::Util::instanceOf<NodeFunctionFieldAccess>(*itr))
        {
            const std::shared_ptr<NodeFunctionFieldAccess> accessNodeFunction = NES::Util::as<NodeFunctionFieldAccess>(*itr);
            fieldsInPredicate.push_back(accessNodeFunction->getFieldName());
        }
    }

    return fieldsInPredicate;
}

}
