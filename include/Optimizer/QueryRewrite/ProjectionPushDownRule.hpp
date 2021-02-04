/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_IMPL_OPTIMIZER_QUERYREWRITE_PROJECTIONPUSHDOWN_HPP_
#define NES_IMPL_OPTIMIZER_QUERYREWRITE_PROJECTIONPUSHDOWN_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class Node;
typedef std::shared_ptr<Node> NodePtr;

class ProjectionLogicalOperatorNode;
typedef std::shared_ptr<ProjectionLogicalOperatorNode> ProjectionLogicalOperatorNodePtr;

class ProjectionPushDownRule;
typedef std::shared_ptr<ProjectionPushDownRule> ProjectionPushDownRulePtr;

/**
 * @brief This class is responsible for altering the query plan to push down the projection as much as possible.
 * Following are the exceptions:
 *  1.) The Leaf node in the query plan will always be source node. This means the projection can't be push below a source node.
 *  2.) If their exists another projection operator next to the target projection operator then it can't be pushed down below that
 *      specific projection operator.
 */
class ProjectionPushDownRule : public BaseRewriteRule {

  public:
    QueryPlanPtr apply(QueryPlanPtr queryPlanPtr) override;

    static ProjectionPushDownRulePtr create();

  private:
    explicit ProjectionPushDownRule();

    /**
     * @brief Push down given projection operator as close to the source operator as possible
     * @param projectionOperator
     */
    void pushDownProjection(ProjectionLogicalOperatorNodePtr projectionOperator);

    /**
     * @brief Get the name of the field manipulated by the Map operator
     * @param Operator pointer
     * @return name of the field
     */
    std::string getFieldNameUsedByMapOperator(NodePtr node) const;

    /**
     * @brief Get the name of the key field manipulated by the join  operator
     * @param Operator pointer
     * @return name of the key field
     */
    std::string getFieldNameUsedByJoinOperator(NodePtr node) const;

    /**
     * @brief Validate if the input field is used in the projection attributes of the operator
     * @param projectionOperator : projection operator whose attribute fields need to be checked
     * @param fieldName :  name of the field to be checked
     * @return true if field use in the projection attributes else false
     */
    bool isFieldsMatchingInProjectionAttributes(ProjectionLogicalOperatorNodePtr projectionOperator, const std::string fieldName) const;
};

}// namespace NES

#endif//NES_IMPL_OPTIMIZER_QUERYREWRITE_PROJECTIONPUSHDOWN_HPP_
