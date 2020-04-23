#ifndef NES_INCLUDE_NODES_PHASES_TYPEINFERENCEPHASE_HPP_
#define NES_INCLUDE_NODES_PHASES_TYPEINFERENCEPHASE_HPP_
#include <memory>
namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class Node;
typedef std::shared_ptr<Node> NodePtr;
class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class TypeInferencePhase {
  public:
    static TypeInferencePhasePtr create();
    OperatorNodePtr transform(OperatorNodePtr operatorNode);
  private:
    TypeInferencePhase();
    void inferOperatorType(NodePtr operatorNode);
};
}

#endif //NES_INCLUDE_NODES_PHASES_TYPEINFERENCEPHASE_HPP_
