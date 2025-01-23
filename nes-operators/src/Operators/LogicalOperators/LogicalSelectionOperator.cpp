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
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

LogicalSelectionOperator::LogicalSelectionOperator(NodeFunctionPtr predicate, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), predicate(std::move(predicate))
{
}

NodeFunctionPtr LogicalSelectionOperator::getPredicate() const
{
    return predicate;
}

void LogicalSelectionOperator::setPredicate(NodeFunctionPtr newPredicate)
{
    predicate = std::move(newPredicate);
}

bool LogicalSelectionOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalSelectionOperator>(rhs)->getId() == id;
}

bool LogicalSelectionOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<LogicalSelectionOperator>(rhs))
    {
        auto filterOperator = NES::Util::as<LogicalSelectionOperator>(rhs);
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
    predicate->inferStamp(*inputSchema);
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
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalSelectionOperator::inferStringSignature()
{
    std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalSelectionOperator: Inferring String signature for {}", *operatorNode);
    INVARIANT(!children.empty(), "LogicalSelectionOperator: Filter should have children");

    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "FILTER(" << *predicate << ")." << *childSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
float LogicalSelectionOperator::getSelectivity() const
{
    return selectivity;
}
void LogicalSelectionOperator::setSelectivity(float newSelectivity)
{
    selectivity = newSelectivity;
}

std::vector<std::string> LogicalSelectionOperator::getFieldNamesUsedByFilterPredicate() const
{
    NES_TRACE("LogicalSelectionOperator: Find all field names used in filter operator");

    ///vector to save the names of all the fields that are used in this predicate
    std::vector<std::string> fieldsInPredicate;

    ///iterator to go over all the fields of the predicate
    DepthFirstNodeIterator depthFirstNodeIterator(predicate);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr)
    {
        ///NodeFunctionif it finds a fieldAccess this means that the predicate uses this specific field that comes from any source
        if (NES::Util::instanceOf<NodeFunctionFieldAccess>(*itr))
        {
            const NodeFunctionFieldAccessPtr accessNodeFunction = NES::Util::as<NodeFunctionFieldAccess>(*itr);
            fieldsInPredicate.push_back(accessNodeFunction->getFieldName());
        }
    }

    return fieldsInPredicate;
}

}
