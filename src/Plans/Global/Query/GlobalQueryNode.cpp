#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>

namespace NES {

template<class NodeType>
void GlobalQueryNode::getNodesWithTypeHelper(std::vector<GlobalQueryNodePtr>& foundNodes) {
    if (logicalOperators[0]->instanceOf<NodeType>()) {
        foundNodes.push_back(std::shared_ptr<GlobalQueryNode>(this));
    }
    for (auto& successor : this->children) {
        successor->as<GlobalQueryNode>()->getNodesWithTypeHelper<GlobalQueryNode>(foundNodes);
    }
}

}// namespace NES