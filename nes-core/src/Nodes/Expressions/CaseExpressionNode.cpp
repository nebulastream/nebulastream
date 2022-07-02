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
#include <Nodes/Expressions/CaseExpressionNode.hpp>
#include <utility>

namespace NES {
        CaseExpressionNode::CaseExpressionNode(DataTypePtr stamp) : ExpressionNode(std::move(stamp)) {}

        CaseExpressionNode::CaseExpressionNode(CaseExpressionNode* other) : ExpressionNode(other) {
            for (auto leftItr = getLeft().begin(); leftItr != getLeft().end(); ++leftItr) {
                addChildWithEqual(leftItr->get()->copy());
            }

            addChildWithEqual(getRight()->copy());
        }

        ExpressionNodePtr CaseExpressionNode::create(std::vector<ExpressionNodePtr> const& left, const ExpressionNodePtr& right) {
            auto caseNode = std::make_shared<CaseExpressionNode>(right->getStamp());
            caseNode->setChildren(left,right);
            return caseNode;
        }

        void CaseExpressionNode::inferStamp(SchemaPtr schema) {
            auto left = getLeft();
            auto right = getRight();
            right->inferStamp(schema);
            if (right->getStamp()->isUndefined()){
                throw std::logic_error(
                    "CaseExpressionNode: Error during stamp inference. Right type must be defined, but was:"
                    + right->getStamp()->toString()
                );
            }

            for (auto elem : left){
                elem->inferStamp(schema);
                //all elements must have same stamp as right value
                if (!right->getStamp()->isEquals(elem->getStamp())) {
                    throw std::logic_error(
                        "CaseExpressionNode: Error during stamp inference."
                        "All elements must have same stamp as right default value, but element " + elem->toString() + " has:"
                        + elem->getStamp()->toString() + "."
                        "Right was: " + right->getStamp()->toString()
                    );
                }
            }

            stamp = right->getStamp();
            NES_TRACE("CaseExpressionNode: we assigned the following stamp: " << stamp->toString());
        }

        void CaseExpressionNode::setChildren(std::vector<ExpressionNodePtr> const& left, ExpressionNodePtr const& right) {
            for (auto elem : left){
                addChildWithEqual(elem);
            };
            addChildWithEqual(right);
        }

        std::vector<ExpressionNodePtr> CaseExpressionNode::getLeft() const {
            if (children.size() <= 1) {
                NES_FATAL_ERROR("A case expression always should have at least two children, but it had: " << children.size());
            }
            //todo: maybe replace by vector slicing
            std::vector<ExpressionNodePtr> leftChildren;
            for ( auto elem : children){
                leftChildren.push_back(elem->as<ExpressionNode>());
            };

            return leftChildren;
        }

        ExpressionNodePtr CaseExpressionNode::getRight() const {
            if (children.size() <= 1) {
                NES_FATAL_ERROR("A case expression always should have at least two children, but it had: " << children.size());
            }
            return (*children.end())->as<ExpressionNode>();
        }

        bool CaseExpressionNode::equal(NodePtr const& rhs) const {
            if ( rhs->instanceOf<CaseExpressionNode>()) {
                auto otherCaseNode = rhs->as<CaseExpressionNode>();
                if (getChildren().size() != otherCaseNode->getChildren().size()) {
                    return false;
                }
                for (std::size_t i = 0; i < getChildren().size(); i++){
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
            std::vector<ExpressionNodePtr> left = getLeft();
            for (std::size_t i = 0; i < left.size()-1; i++){
                ss << left[i]->toString() << ",";
            }
            ss << (*left.end())->toString() << "}," << getRight()->toString();

            return ss.str();
        }

        ExpressionNodePtr  CaseExpressionNode::copy() {
            return std::make_shared<CaseExpressionNode>(CaseExpressionNode(this));
        }

    }// namespace NES