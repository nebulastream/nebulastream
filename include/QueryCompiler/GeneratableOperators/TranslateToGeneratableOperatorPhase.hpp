#ifndef NES_INCLUDE_NODES_PHASES_TRANSLATETOGENERATABLEOPERATORS_HPP_
#define NES_INCLUDE_NODES_PHASES_TRANSLATETOGENERATABLEOPERATORS_HPP_

#include <memory>

namespace NES {

class TranslateToGeneratableOperatorPhase;
typedef std::shared_ptr<TranslateToGeneratableOperatorPhase> TranslateToGeneratableOperatorPhasePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class GeneratableOperator;
typedef std::shared_ptr<GeneratableOperator> GeneratableOperatorPtr;

class NodeEngine;
typedef std::shared_ptr<NodeEngine> NodeEnginePtr;


/**
 * @brief Translates a logical query plan to the legacy operator tree
 */
class TranslateToGeneratableOperatorPhase {
  public:
    /**
     * @brief Factory method to create a translator phase.
     */
    static TranslateToGeneratableOperatorPhasePtr create();
    /**
     * @brief Translates a operator node and all its children to the legacy representation.
     * @param operatorNode
     * @return Legacy Operator Tree
     */
    OperatorNodePtr transform(OperatorNodePtr operatorNode, NodeEnginePtr nodeEngine, OperatorNodePtr legacyParent = nullptr);


    /**
     * @brief Translates an individual operator to its legacy representation.
     * @param operatorNode
     * @return Legacy Operator
     */
    OperatorNodePtr transformIndividualOperator(OperatorNodePtr operatorNode, NodeEnginePtr nodeEngine, OperatorNodePtr generatableParentOperator);

    TranslateToGeneratableOperatorPhase();

};

}// namespace NES

#endif//NES_INCLUDE_NODES_PHASES_TRANSLATETOGENERATABLEOPERATORS_HPP_
