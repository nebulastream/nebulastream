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
