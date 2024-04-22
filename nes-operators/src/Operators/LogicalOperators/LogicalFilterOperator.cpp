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

#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {

LogicalFilterOperator::LogicalFilterOperator(ExpressionNodePtr const& predicate, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), predicate(predicate) {}

ExpressionNodePtr LogicalFilterOperator::getPredicate() const { return predicate; }

void LogicalFilterOperator::setPredicate(ExpressionNodePtr newPredicate) { predicate = std::move(newPredicate); }

bool LogicalFilterOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalFilterOperator>()->getId() == id;
}

bool LogicalFilterOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalFilterOperator>()) {
        auto filterOperator = rhs->as<LogicalFilterOperator>();
        return predicate->equal(filterOperator->predicate);
    }
    return false;
};

std::string LogicalFilterOperator::toString() const {
    std::stringstream ss;
    ss << "FILTER(opId: " << id << ", statisticId: " << statisticId << ", predicate: "<< predicate->toString() <<")";
    return ss.str();
}

bool LogicalFilterOperator::inferSchema() {
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }
    predicate->inferStamp( inputSchema);
    if (!predicate->isPredicate()) {
        NES_THROW_RUNTIME_ERROR("FilterLogicalOperator: the filter expression is not a valid predicate");
    }
    return true;
}

OperatorPtr LogicalFilterOperator::copy() {
    auto copy = LogicalOperatorFactory::createFilterOperator(predicate->copy(), id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    copy->setStatisticId(statisticId);
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalFilterOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("LogicalFilterOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty(), "LogicalFilterOperator: Filter should have children");

    //Infer query signatures for child operators
    for (const auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << "FILTER(" + predicate->toString() + ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
float LogicalFilterOperator::getSelectivity() const { return selectivity; }
void LogicalFilterOperator::setSelectivity(float newSelectivity) { selectivity = newSelectivity; }

std::vector<std::string> LogicalFilterOperator::getFieldNamesUsedByFilterPredicate() const {
    NES_TRACE("LogicalFilterOperator: Find all field names used in filter operator");

    //vector to save the names of all the fields that are used in this predicate
    std::vector<std::string> fieldsInPredicate;

    //iterator to go over all the fields of the predicate
    DepthFirstNodeIterator depthFirstNodeIterator(predicate);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
        //if it finds a fieldAccessExpressionNode this means that the predicate uses this specific field that comes from any source
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            fieldsInPredicate.push_back(accessExpressionNode->getFieldName());
        }
    }

    return fieldsInPredicate;
}
}// namespace NES
