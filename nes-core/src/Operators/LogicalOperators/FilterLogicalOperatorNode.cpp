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

#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>
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

bool FilterLogicalOperatorNode::inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) {
    if (!LogicalUnaryOperatorNode::inferSchema(typeInferencePhaseContext)) {
        return false;
    }
    predicate->inferStamp(typeInferencePhaseContext, inputSchema);
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

std::vector<ExpressionNodePtr> FilterLogicalOperatorNode::splitUpPredicates() {
    return _splitUpPredicates(predicate);
}

std::vector<ExpressionNodePtr> FilterLogicalOperatorNode::_splitUpPredicates(ExpressionNodePtr expressionNode) {
    NES_TRACE("FilterLogicalOperatorNode: Split up filter predicate conjunction into predicates");
    if (expressionNode->instanceOf<AndExpressionNode>()){
        NES_TRACE("FilterLogicalOperatorNode: The filter predicate is a conjunction");
        auto conjunctionPredicates = expressionNode->getChildren();
        NES_TRACE("FilterLogicalOperatorNode: Looping over the two children and applying split up recursively");
        for (auto& conjunctionPredicate : conjunctionPredicates){
            auto subFilterPredicate = this->copy()->as<FilterLogicalOperatorNode>();
            conjunctionPredicate.spl->setPredicate(conjunctionPredicate->as<ExpressionNode>());
            auto subPredicates = subFilterPredicate->splitUpPredicates();
            splitPredicates.insert(splitPredicates.end(), subPredicates.begin(), subPredicates.end());
        }
    }
    else if (expressionNode->instanceOf<NegateExpressionNode>()) {
        if (expressionNode->getChildren()[0]->instanceOf<OrExpressionNode>()){
            NES_TRACE("FilterLogicalOperatorNode: The predicate is of the form !( expression1 || expression2 )");
            NES_TRACE("FilterLogicalOperatorNode: It can be reformulated to ( !expression1 && !expression2 )");
            auto orExpression = expressionNode->getChildren()[0];
            auto negatedChild1 = NegateExpressionNode::create(orExpression->getChildren()[0]->as<ExpressionNode>());
            auto negatedChild2 = NegateExpressionNode::create(orExpression->getChildren()[1]->as<ExpressionNode>());
            auto equivalentAndExpression = AndExpressionNode::create(negatedChild1, negatedChild2);
            NES_TRACE("FilterLogicalOperatorNode: Changing predicate to equivalent AndExpression");
            this->setPredicate(equivalentAndExpression);
            return this->splitUpPredicates();
        }
        else if (expressionNode->getChildren()[0]->instanceOf<NegateExpressionNode>()){
            NES_TRACE("FilterLogicalOperatorNode: The predicate is of the form (!!expression)");
            NES_TRACE("FilterLogicalOperatorNode: It can be reformulated to (expression)");
            // getPredicate() is the first NegateExpression; first getChildren()[0] is the second NegateExpression;
            // second getChildren()[0] is the expressionNode that was negated twice. copy() only copies children of this expressionNode. (probably not mandatory but no reference to the negations needs to be kept)
            this->setPredicate(expressionNode->getChildren()[0]->getChildren()[0]->as<ExpressionNode>()->copy());
            return this->splitUpPredicates();
        }
    }
    else{
        NES_TRACE("FilterLogicalOperatorNode: The filter predicate cannot be split, returning original predicate");
        std::vector<ExpressionNodePtr> splitPredicates;
        splitPredicates.push_back(this->as<FilterLogicalOperatorNode>()->getPredicate());
        return splitPredicates;
    }
}

}// namespace NES
