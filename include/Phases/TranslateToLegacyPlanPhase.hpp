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

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

/**
 * @brief Translates a logical query plan to the legacy operator tree
 */
class TranslateToLegacyPlanPhase {
  public:
    /**
     * @brief Factory method to create a translator phase.
     */
    static TranslateToLegacyPlanPhasePtr create(BufferManagerPtr bufferManagerPtr);
    /**
     * @brief Translates a operator node and all its children to the legacy representation.
     * @param operatorNode
     * @return Legacy Operator Tree
     */
    OperatorPtr transform(OperatorNodePtr operatorNode);

  private:

    TranslateToLegacyPlanPhase(BufferManagerPtr bufferManager);

    /**
     * @brief Translates an individual operator to its legacy representation.
     * @param operatorNode
     * @return Legacy Operator
     */
    OperatorPtr transformIndividualOperator(OperatorNodePtr operatorNode);
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

    BufferManagerPtr bufferManager;
};

}// namespace NES

#endif//NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_
