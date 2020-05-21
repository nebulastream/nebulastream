
#include <API/Schema.hpp>
#include <GRPC/Serialization/OperatorSerializationUtil.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <SerializableOperator.pb.h>

namespace NES {

SerializableOperatorPtr OperatorSerializationUtil::serialize(QueryPlanPtr plan) {
    auto ope = std::shared_ptr<SerializableOperator>();
    auto rootOperator = plan->getRootOperator();
    serializeOperator(rootOperator, ope.get());
    return ope;
}

}// namespace NES