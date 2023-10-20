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
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {

FilterLogicalOperatorNode::FilterLogicalOperatorNode(ExpressionNodePtr const& predicate, uint64_t id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), predicate(predicate) {
    selectivity = 1.0;
}

ExpressionNodePtr FilterLogicalOperatorNode::getPredicate() const { return predicate; }

void FilterLogicalOperatorNode::setPredicate(ExpressionNodePtr newPredicate) { predicate = std::move(newPredicate); }

bool FilterLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<FilterLogicalOperatorNode>()->getId() == id;
}

bool FilterLogicalOperatorNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterOperator = rhs->as<FilterLogicalOperatorNode>();
        return predicate->equal(filterOperator->predicate);
    }
    return false;
};

std::string FilterLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "FILTER(" << id << ")";
    return ss.str();
}

bool FilterLogicalOperatorNode::inferSchema() {
    if (!LogicalUnaryOperatorNode::inferSchema()) {
        return false;
    }
    predicate->inferStamp( inputSchema);
    if (!predicate->isPredicate()) {
        NES_THROW_RUNTIME_ERROR("FilterLogicalOperator: the filter expression is not a valid predicate");
    }
    return true;
}

OperatorNodePtr FilterLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createFilterOperator(predicate, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void FilterLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("FilterLogicalOperatorNode: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty(), "FilterLogicalOperatorNode: Filter should have children");

    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperatorNode>()->getHashBasedSignature();
    signatureStream << "FILTER(" + predicate->toString() + ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
float FilterLogicalOperatorNode::getSelectivity() { return selectivity; }
void FilterLogicalOperatorNode::setSelectivity(float newSelectivity) { selectivity = newSelectivity; }

std::vector<std::string> FilterLogicalOperatorNode::getFieldNamesUsedByFilterPredicate() {
    NES_TRACE("FilterLogicalOperatorNode: Find all field names used in filter operator");

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
