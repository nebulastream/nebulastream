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
#include <Common/DataTypes/DataType.hpp>
#include <Expressions/CaseExpressionNode.hpp>
#include <Expressions/WhenExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>
namespace NES {
CaseExpressionNode::CaseExpressionNode(DataTypePtr stamp) : ExpressionNode(std::move(stamp)) {}

CaseExpressionNode::CaseExpressionNode(CaseExpressionNode* other) : ExpressionNode(other) {
    auto otherWhenChildren = getWhenChildren();
    for (auto& whenItr : otherWhenChildren) {
        addChildWithEqual(whenItr->copy());
    }
    addChildWithEqual(getDefaultExp()->copy());
}

ExpressionNodePtr CaseExpressionNode::create(std::vector<ExpressionNodePtr> const& whenExps,
                                             const ExpressionNodePtr& defaultExp) {
    auto caseNode = std::make_shared<CaseExpressionNode>(defaultExp->getStamp());
    caseNode->setChildren(whenExps, defaultExp);
    return caseNode;
}

void CaseExpressionNode::inferStamp( SchemaPtr schema) {

    auto whenChildren = getWhenChildren();
    auto defaultExp = getDefaultExp();
    defaultExp->inferStamp( schema);
    if (defaultExp->getStamp()->isUndefined()) {
        NES_THROW_RUNTIME_ERROR("Error during stamp inference. Right type must be defined, but was: {}",
                                defaultExp->getStamp()->toString());
    }

    for (auto elem : whenChildren) {
        elem->inferStamp( schema);
        //all elements in whenChildren must be WhenExpressionNodes
        if (!elem->instanceOf<WhenExpressionNode>()) {
            NES_THROW_RUNTIME_ERROR(
                "Error during stamp inference. All expressions in when expression vector must be when expressions, but "
                + elem->toString() + " is not a when expression.");
        }
        //all elements must have same stamp as defaultExp value
        if (!defaultExp->getStamp()->equals(elem->getStamp())) {
            NES_THROW_RUNTIME_ERROR(
                "Error during stamp inference. All elements must have same stamp as defaultExp default value, but element "
                + elem->toString() + " has: " + elem->getStamp()->toString()
                + ". Right was: " + defaultExp->getStamp()->toString());
        }
    }

    stamp = defaultExp->getStamp();
    NES_TRACE("CaseExpressionNode: we assigned the following stamp: {}", stamp->toString());
}

void CaseExpressionNode::setChildren(std::vector<ExpressionNodePtr> const& whenExps, ExpressionNodePtr const& defaultExp) {
    for (auto elem : whenExps) {
        addChildWithEqual(elem);
    }
    addChildWithEqual(defaultExp);
}

std::vector<ExpressionNodePtr> CaseExpressionNode::getWhenChildren() const {
    if (children.size() < 2) {
        NES_FATAL_ERROR("A case expression always should have at least two children, but it had: {}", children.size());
    }
    std::vector<ExpressionNodePtr> whenChildren;
    for (auto whenIter = children.begin(); whenIter != children.end() - 1; ++whenIter) {
        whenChildren.push_back(whenIter->get()->as<ExpressionNode>());
    }
    return whenChildren;
}

ExpressionNodePtr CaseExpressionNode::getDefaultExp() const {
    if (children.size() <= 1) {
        NES_FATAL_ERROR("A case expression always should have at least two children, but it had: {}", children.size());
    }
    return (*(children.end() - 1))->as<ExpressionNode>();
}

bool CaseExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<CaseExpressionNode>()) {
        auto otherCaseNode = rhs->as<CaseExpressionNode>();
        if (getChildren().size() != otherCaseNode->getChildren().size()) {
            return false;
        }
        for (std::size_t i = 0; i < getChildren().size(); i++) {
            if (!getChildren().at(i)->equal(otherCaseNode->getChildren().at(i))) {
                return false;
            }
        }
        return true;
    }
    return false;
}

std::string CaseExpressionNode::toString() const {
    std::stringstream ss;
    ss << "CASE({";
    std::vector<ExpressionNodePtr> left = getWhenChildren();
    for (std::size_t i = 0; i < left.size() - 1; i++) {
        ss << left.at(i)->toString() << ",";
    }
    ss << (*(left.end() - 1))->toString() << "}," << getDefaultExp()->toString();

    return ss.str();
}

ExpressionNodePtr CaseExpressionNode::copy() {
    std::vector<ExpressionNodePtr> copyOfWhenExpressions;
    for (auto whenExpression : getWhenChildren()) {
        copyOfWhenExpressions.push_back(whenExpression->as<ExpressionNode>()->copy());
    }
    return CaseExpressionNode::create(copyOfWhenExpressions, getDefaultExp()->copy());
}

}// namespace NES