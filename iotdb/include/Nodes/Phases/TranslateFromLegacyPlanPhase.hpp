#ifndef NES_INCLUDE_NODES_PHASES_TRANSLATEFROMLEGECYPLAN_HPP_
#define NES_INCLUDE_NODES_PHASES_TRANSLATEFROMLEGECYPLAN_HPP_

#include <memory>

namespace NES {

class TranslateFromLegacyPlanPhase;
typedef std::shared_ptr<TranslateFromLegacyPlanPhase> TranslateFromLegacyPlanPhasePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class UserAPIExpression;
typedef std::shared_ptr<UserAPIExpression> UserAPIExpressionPtr;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

/**
 * @brief Translates a logical query plan to the legacy operator tree
 */
class TranslateFromLegacyPlanPhase {
  public:
    /**
     * @brief Factory method to create a translator phase.
     */
    static TranslateFromLegacyPlanPhasePtr create();
    /**
     * @brief Translates a operator node and all its children to the legacy representation.
     * @param Legacy operator
     * @return Operator Node
     */
    OperatorNodePtr transform(OperatorPtr operatorPtr);
  private:

    /**
     * @brief Translates an individual operator to its legacy representation.
     * @param operatorNode
     * @return Legacy Operator
     */
    OperatorNodePtr transformIndividualOperator(OperatorPtr operatorPtr);

    /**
     * @brief Translates an expression to a legacy user api expression.
     * @param expression node
     * @return UserAPIExpressionPtr
     */
     ExpressionNodePtr transformToExpression(UserAPIExpressionPtr expressionPtr);

    /**
     * @brief Translates logical expessions to a legacy user api expression.
     * @param expression node
     * @return UserAPIExpressionPtr
     */
    ExpressionNodePtr transformLogicalExpressions(PredicatePtr expressionPtr);

    /**
     * @brief Translates arithmetical expessions to a legacy user api expression.
     * @param expression node
     * @return UserAPIExpressionPtr
     */
    ExpressionNodePtr transformArithmeticalExpressions(PredicatePtr expressionPtr);
};

}

#endif //NES_INCLUDE_NODES_PHASES_TRANSLATEFROMLEGECYPLAN_HPP_
