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
 * @brief Translates a legacy operator tree into a logical query plan
 */
class TranslateFromLegacyPlanPhase {
  public:
    /**
     * @brief Factory method to create a translator phase.
     */
    static TranslateFromLegacyPlanPhasePtr create();
    /**
     * @brief Translates a legacy operator and all its children to the node operator  representation.
     * @param Legacy operator
     * @return Operator Node
     */
    OperatorNodePtr transform(OperatorPtr operatorPtr);
  private:

    /**
     * @brief Translates an individual operator to its legacy representation.
     * @param Legacy Operator
     * @return operatorNode
     */
    OperatorNodePtr transformIndividualOperator(OperatorPtr operatorPtr);

    /**
     * @brief Translates a legacy user api expression to an expression.
     * @param UserAPIExpressionPtr
     * @return expression node
     */
    ExpressionNodePtr transformToExpression(UserAPIExpressionPtr expressionPtr);

    /**
     * @brief Translates legacy predicate into a logical expession.
     * @param legacy predicate
     * @return expression node
     */
    ExpressionNodePtr transformLogicalExpressions(PredicatePtr expressionPtr);

    /**
     * @brief Translates legacy arithmetical predicate into logical expession.
     * @param legacy predicate
     * @return expression node
     */
    ExpressionNodePtr transformArithmeticalExpressions(PredicatePtr expressionPtr);
};

}

#endif //NES_INCLUDE_NODES_PHASES_TRANSLATEFROMLEGECYPLAN_HPP_
