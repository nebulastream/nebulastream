#ifndef NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_
#define NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_

#include <memory>
#include <QueryCompiler/QueryCompiler.hpp>

namespace NES {

class TranslateToLegacyPlanPhase;
typedef std::shared_ptr<TranslateToLegacyPlanPhase> TranslateToLegacyPlanPhasePtr;

class Node;
typedef std::shared_ptr<Node> NodePtr;

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;


class TranslateToLegacyPlanPhase {
  public:
    static TranslateToLegacyPlanPhasePtr create();
    OperatorPtr transform(NodePtr node);
};

}

#endif //NES_INCLUDE_NODES_PHASES_TRANSLATETOLEGECYPLAN_HPP_
