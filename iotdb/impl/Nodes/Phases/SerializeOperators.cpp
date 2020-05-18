

#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <SerializableOperator.pb.h>
namespace NES {
class SerializeOperators {
    void serialize(QueryPlanPtr plan) {
        auto ope = SerializableOperator();
    }
};
}// namespace NES
