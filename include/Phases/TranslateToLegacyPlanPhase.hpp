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

#ifndef NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_
#define NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_

#include <memory>

namespace NES {

class TranslateToLegacyPlanPhase;
typedef std::shared_ptr<TranslateToLegacyPlanPhase> TranslateToLegacyPlanPhasePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class UserAPIExpression;
typedef std::shared_ptr<UserAPIExpression> UserAPIExpressionPtr;

class NodeEngine;
typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

/**
 * @brief Translates a logical query plan to the legacy operator tree
 */
class TranslateToLegacyPlanPhase {
  public:
    /**
     * @brief Factory method to create a translator phase.
     */
    static TranslateToLegacyPlanPhasePtr create();

    TranslateToLegacyPlanPhase();

    /**
     * @brief Translates an expression to a legacy user api expression.
     * @param expression node
     * @return UserAPIExpressionPtr
     */
    UserAPIExpressionPtr transformExpression(ExpressionNodePtr node);

    /**
     * @brief Translates logical expessions to a legacy user api expression.
     * @param expression node
     * @return UserAPIExpressionPtr
     */
    UserAPIExpressionPtr transformLogicalExpressions(ExpressionNodePtr node);

    /**
     * @brief Translates arithmetical expessions to a legacy user api expression.
     * @param expression node
     * @return UserAPIExpressionPtr
     */
    UserAPIExpressionPtr transformArithmeticalExpressions(ExpressionNodePtr node);
};

}// namespace NES

#endif//NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_
