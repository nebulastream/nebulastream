
#include <Nodes/Phases/TypeInferencePhase.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Operators/OperatorNode.hpp>

namespace NES {

TypeInferencePhase::TypeInferencePhase() {}

TypeInferencePhasePtr TypeInferencePhase::create() {
    return std::make_shared<TypeInferencePhase>(TypeInferencePhase());
}

OperatorNodePtr TypeInferencePhase::transform(OperatorNodePtr operatorNode) {
    auto iterator = BreadthFirstNodeIterator(operatorNode);
    for (const auto& node:iterator) {
        inferOperatorType(node);
    }
    return operatorNode;
}

void TypeInferencePhase::inferOperatorType(NodePtr node) {
    if(node->instanceOf<MapLogicalOperatorNode>()){
        auto mapOperator = node->as<MapLogicalOperatorNode>();
        auto resultSchema = mapOperator->getResultSchema();

    }
}

}